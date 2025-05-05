"""
Genetic Algorithm Scheduler
==========================

This module implements a genetic algorithm to optimize the scheduling of manufacturing operations.
Key features:
- Uses priority-based scheduling of orders
- Allows for operation splitting across multiple resources when available
- Enforces sequence constraints (operations must be performed in order)
- Ensures operations are scheduled within resource availability windows
- Handles resource availability windows for both machines and workers
- Utilizes multithreading for evaluating multiple schedules in parallel
- Optimizes operation splitting through genetic evolution
- Rounds operation times to configurable intervals (default 30 minutes)

The scheduler works in conjunction with the resource scheduler module which:
- Provides availability windows for each resource (machines and workers)
- Determines the earliest available time slot within these windows for each operation
- Ensures operations are only scheduled during valid resource availability periods
"""

import random
import copy
import time
import uuid
import datetime
from datetime import timedelta
from typing import Dict, List, Tuple, Any
import threading
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import partial
from setup_time_calculator import calculate_setup_time, update_resource_usage

# Import the data operations and resource scheduler modules
from data_operations import fetch_all_data, prepare_data_for_optimization, fetch_unassigned_tasks, fetch_tasks_for_run
from resource_scheduler import find_earliest_resource_availability, schedule_within_availability
# Import the functions to save the schedule and manage optimization runs
from save_schedule import save_schedule_to_db, create_optimization_run, update_optimization_run

# Configuration for time interval rounding
TIME_INTERVAL_MINUTES = 30  # Default to 30-minute intervals

def round_to_interval(dt: datetime.datetime, interval_minutes: int = TIME_INTERVAL_MINUTES) -> datetime.datetime:
    """
    Round a datetime to the nearest interval.
    
    Args:
        dt: The datetime to round
        interval_minutes: The interval in minutes to round to (default: TIME_INTERVAL_MINUTES)
        
    Returns:
        The rounded datetime
    """
    # Calculate the number of minutes to add to round up
    minutes_to_add = (interval_minutes - (dt.minute % interval_minutes)) % interval_minutes
    if minutes_to_add == 0:
        return dt.replace(second=0, microsecond=0)
    return (dt + timedelta(minutes=minutes_to_add)).replace(second=0, microsecond=0)

def round_duration_to_interval(duration_hours: float, interval_minutes: int = TIME_INTERVAL_MINUTES) -> float:
    """
    Round a duration in hours up to the nearest interval.
    
    Args:
        duration_hours: The duration in hours
        interval_minutes: The interval in minutes to round to (default: TIME_INTERVAL_MINUTES)
        
    Returns:
        The rounded duration in hours
    """
    duration_minutes = duration_hours * 60
    rounded_minutes = ((int(duration_minutes) + interval_minutes - 1) // interval_minutes) * interval_minutes
    return rounded_minutes / 60

# Standalone worker function for parallel evaluation of individuals
def evaluate_individual_worker(individual, start_date, data, scheduling_data, orders, operations_by_product, planning_horizon_end, buffer_time_hours, initial_resource_usage=None):
    """
    Standalone worker function for parallel evaluation of individuals.
    This function needs to be at module level for multiprocessing to work properly.
    
    Args:
        individual: The individual to evaluate
        Additional parameters to recreate the evaluation context
        initial_resource_usage: Pre-initialized resource usage from unassigned tasks
        
    Returns:
        Dict containing evaluation results
    """
    # Create a new resource usage tracker for this evaluation
    # Start with a deep copy of the initial resource usage if provided
    if initial_resource_usage:
        local_resource_usage = copy.deepcopy(initial_resource_usage)
    else:
        local_resource_usage = {
            'machines': {},
            'workers': {}
        }
    
    # Extract the scheduling parameters
    order_sequence = individual.get('order_sequence', [])
    operation_splits = individual.get('operation_splits', {})
    
    # Initialize tracking variables
    schedule = []  # List to store all scheduled operations
    makespan_end = start_date  # Track the end time of the last operation
    total_tardiness = timedelta()  # Track total tardiness
    
    # Track metrics per order
    order_metrics = {}  # order_id -> {start_time, end_time, is_on_time, tardiness}
    
    # Track operations for each order
    order_operations = {}  # order_id -> {last_operation_end, operations, sequence_ends}
    
    # Process orders in sequence
    for order_idx in order_sequence:
        if order_idx >= len(orders):
            continue
            
        order = orders[order_idx]
        order_id = str(order['id'])
        product_id = order['product_id']
        required_date = order.get('required_date')
        priority = order.get('sales_order_priority', 1)  # Default to 1 if priority not set
        
        # Initialize order metrics
        order_metrics[order_id] = {
            'start_time': None,
            'end_time': None,
            'is_on_time': True,  # Assume on time until proven late
            'tardiness': timedelta(0),
            'required_date': required_date,
            'priority': priority
        }
        
        # Skip if no product
        if not product_id:
            continue
        
        # Get operations for this product
        if product_id not in operations_by_product:
            continue
            
        operations = operations_by_product[product_id]
        
        # Group operations by sequence number
        sequence_groups = {}
        for op in operations:
            # Skip operations that don't belong to this order
            if op.get('order_id') != order_id:
                continue
                
            seq_num = op['sequence_number']
            if seq_num not in sequence_groups:
                sequence_groups[seq_num] = []
            sequence_groups[seq_num].append(op)
        
        # Sort sequence numbers
        sequence_numbers = sorted(sequence_groups.keys())
        
        # Initialize order operations tracking
        if order_id not in order_operations:
            order_operations[order_id] = {
                'last_operation_end': start_date,
                'operations': {},
                'sequence_ends': {}  # Track end times for each sequence number
            }
        
        # Process each sequence group
        for seq_num in sequence_numbers:
            parallel_ops = sequence_groups[seq_num]
            
            # Track the end times of all operations in this sequence
            sequence_end_times = []
            
            # Get the earliest time operations in this sequence can start
            # For first sequence, use start_date
            # For subsequent sequences, use the latest end time of all operations in previous sequence
            if seq_num == min(sequence_numbers):
                order_operations[order_id]['current_sequence_start'] = start_date
            else:
                # Get the end time of all operations in the previous sequence
                prev_seq = max([s for s in sequence_numbers if s < seq_num])
                prev_seq_end = order_operations[order_id]['sequence_ends'].get(prev_seq)
                if prev_seq_end:
                    order_operations[order_id]['current_sequence_start'] = prev_seq_end
            
            sequence_start = order_operations[order_id]['current_sequence_start']
            
            # Process all operations in this sequence group
            for op in parallel_ops:
                operation_id = str(op['id'])
                operation_uuid = op['operation_id']
                if isinstance(operation_uuid, str):
                    operation_uuid = uuid.UUID(operation_uuid)
                
                # Get the remaining quantity for this operation
                remaining_quantity = max(1, op.get('remaining_quantity', 0))  # Ensure minimum quantity of 1
                
                # Skip if no remaining quantity to process
                if remaining_quantity <= 0:
                    continue
                
                # Extract required_date for this order
                order_required_date = order_metrics[order_id].get('required_date')
                
                # Update order start time if this is the first operation
                if order_metrics[order_id]['start_time'] is None:
                    order_metrics[order_id]['start_time'] = sequence_start
                
                # Get the number of splits from the individual's chromosome
                desired_splits = max(1, operation_splits.get(operation_id, 1))  # Ensure minimum splits of 1
                
                # Check available resources and limit splits if necessary
                available_resources = find_earliest_resource_availability(
                    operation_uuid,
                    sequence_start,
                    scheduling_data['resource_schedules'],
                    scheduling_data['capability_mappings'],
                    local_resource_usage
                )
                
                if available_resources['status'] != 'success':
                    continue
                
                # Get the number of available resources
                operation_resources = scheduling_data['capability_mappings']['operation_resources'].get(operation_uuid, {})
                available_machines = operation_resources.get('machines', [])
                available_workers = operation_resources.get('setup_and_operation_workers', [])
                
                # Calculate max available based on operation requirements
                operation_requirements = scheduling_data['capability_mappings']['operation_requirements'].get(operation_uuid, {})
                machine_required = operation_requirements.get('machine_required', False)
                worker_required = operation_requirements.get('worker_required')
                
                if machine_required and worker_required:
                    max_available = min(len(available_machines), len(available_workers))
                elif machine_required:
                    max_available = len(available_machines)
                elif worker_required:
                    max_available = len(available_workers)
                else:
                    max_available = 1  # No resources required
                
                # Ensure at least one resource is available if required
                if max_available < 1 and (machine_required or worker_required):
                    continue
                
                # Limit splits by available resources
                max_parallel = min(desired_splits, max_available) if max_available > 0 else 1
                
                # Calculate work units with even splitting
                work_units = []
                base_quantity = remaining_quantity / max_parallel
                remaining_split_quantity = remaining_quantity
                
                for i in range(max_parallel):
                    # For the last unit, use all remaining quantity
                    if i == max_parallel - 1:
                        unit_quantity = remaining_split_quantity
                    else:
                        unit_quantity = base_quantity
                        remaining_split_quantity -= base_quantity
                    work_units.append(unit_quantity)
                
                # Schedule each work unit
                unit_schedules = []
                
                for unit_idx, unit_quantity in enumerate(work_units):
                    schedule_entries = schedule_within_availability(
                        operation_uuid,
                        unit_quantity,
                        op,
                        sequence_start,
                        scheduling_data['resource_schedules'],
                        local_resource_usage,
                        scheduling_data,
                        planning_horizon_end
                    )
                    
                    if not schedule_entries:
                        continue
                    
                    for entry_idx, entry in enumerate(schedule_entries):
                        schedule_entry = {
                            'order_id': order_id,
                            'product_id': product_id,
                            'operation_id': operation_id,
                            'operation_uuid': operation_uuid,
                            'operation_name': op['operation_name'],
                            'sequence_number': seq_num,
                            'unit_idx': unit_idx,
                            'part_idx': entry_idx,
                            'start_time': entry['start_time'],
                            'end_time': entry['end_time'],
                            'machine_id': entry['machine_id'],
                            'worker_id': entry['worker_id'],
                            'quantity': entry['quantity'],
                            'sequence_start': sequence_start,
                            'worker_required': entry['worker_required']  # Add worker requirement type
                        }
                        
                        schedule_entry['setup_time_mins'] = entry['setup_time_mins']
                        schedule_entry['total_duration_mins'] = entry['total_duration_mins']
                        schedule_entry['required_date'] = order_required_date
                        
                        unit_schedules.append(schedule_entry)
                
                if not unit_schedules:
                    continue
                
                order_operations[order_id]['operations'][operation_id] = unit_schedules
                
                schedule.extend(unit_schedules)
                
                op_end_time = max(schedule['end_time'] for schedule in unit_schedules)
                sequence_end_times.append(op_end_time)
            
            if sequence_end_times:
                latest_sequence_end = max(sequence_end_times)
                order_operations[order_id]['last_operation_end'] = latest_sequence_end
                order_operations[order_id]['sequence_ends'][seq_num] = latest_sequence_end
                makespan_end = max(makespan_end, latest_sequence_end)
                
                order_metrics[order_id]['end_time'] = latest_sequence_end
                
                if seq_num == sequence_numbers[-1] and required_date:
                    if isinstance(required_date, datetime.date) and not isinstance(required_date, datetime.datetime):
                        required_date = datetime.datetime.combine(required_date, datetime.time.max)
                    
                    if latest_sequence_end > required_date:
                        tardiness = latest_sequence_end - required_date
                        order_metrics[order_id]['is_on_time'] = False
                        order_metrics[order_id]['tardiness'] = tardiness
                        # Weight the tardiness by the order's priority
                        weighted_tardiness = tardiness * priority * 20000
                        total_tardiness += weighted_tardiness
    
    # Calculate metrics for all orders
    on_time_orders = sum(1 for metrics in order_metrics.values() if metrics['is_on_time'] and metrics['end_time'] is not None)
    total_orders_scheduled = sum(1 for metrics in order_metrics.values() if metrics['end_time'] is not None)
    
    # Calculate average order makespan
    order_makespans = [
        (metrics['end_time'] - metrics['start_time']).total_seconds() / 3600
        for metrics in order_metrics.values()
        if metrics['start_time'] is not None and metrics['end_time'] is not None
    ]
    avg_order_makespan = sum(order_makespans) / len(order_makespans) if order_makespans else 0
    
    # Convert total tardiness to hours
    tardiness_hours = total_tardiness.total_seconds() / 3600
    
    # Calculate fitness
    def calculate_fitness(tardiness, avg_order_makespan):
        tardiness_weight = 10.0
        makespan_weight = 1.0
        
        fitness = (
            tardiness_weight * tardiness +
            makespan_weight * avg_order_makespan
        )
        
        return fitness
    
    fitness = calculate_fitness(
        tardiness_hours,
        avg_order_makespan
    )
    
    return {
        'schedule': schedule,
        'tardiness': tardiness_hours,
        'on_time_orders': on_time_orders,
        'total_orders_scheduled': total_orders_scheduled,
        'avg_order_makespan': avg_order_makespan,
        'order_metrics': order_metrics,
        'fitness': fitness
    }

class GeneticScheduler:
    """
    Class implementing a genetic algorithm for manufacturing operation scheduling.
    """
    
    def __init__(self, 
                 population_size=None,
                 generations=None,
                 crossover_rate=0.8,
                 mutation_rate=0.2,
                 selection_percentage=0.2,
                 elite_size=5,
                 max_workers=None):
        """
        Initialize the genetic scheduler.
        
        Args:
            population_size: Size of the population to evolve (will be overridden by DB settings if None)
            generations: Number of generations to run the algorithm (will be overridden by DB settings if None)
            crossover_rate: Probability of crossover between two individuals
            mutation_rate: Probability of mutation for each individual
            selection_percentage: Percentage of population to select for reproduction
            elite_size: Number of best individuals to preserve
            max_workers: Maximum number of processes to use for parallel evaluation (defaults to CPU count)
        """
        # These will be set from database in prepare_data
        self.population_size = population_size
        self.generations = generations
        
        # These parameters are still hardcoded
        self.crossover_rate = crossover_rate
        self.mutation_rate = mutation_rate
        self.selection_percentage = selection_percentage
        self.elite_size = elite_size
        self.max_workers = max_workers if max_workers is not None else multiprocessing.cpu_count()
        
        # Will be set when optimizing
        self.data = None
        self.scheduling_data = None
        self.orders = None
        self.operations_by_product = {}
        self.start_date = None
        self.buffer_time_hours = None
        self.settings = None
        
        # Lock for thread safety
        self.lock = threading.Lock()
        
        # Add resource usage tracking
        self.resource_usage = {
            'machines': {},  # machine_id -> list of (start_time, end_time) tuples
            'workers': {},    # worker_id -> list of (start_time, end_time) tuples
            'complete_tasks': []  # Store complete task information
        }
        
        print(f"Initialized genetic scheduler with {self.max_workers} parallel workers (cores)")

    def prepare_data(self, start_date=None, planning_horizon_days=120, previous_run_id=None):
        """
        Fetch and prepare data for optimization.
        
        Args:
            start_date: Optional start date for planning, defaults to current date
            planning_horizon_days: Number of days to plan for
            previous_run_id: Optional ID of previous optimization run to use as basis
            
        Returns:
            bool: True if data preparation was successful, False otherwise
        """
        result = fetch_all_data()
        
        if result.get("status") != "success":
            return False
        
        self.data = result["data"]
        
        # Get the latest optimization settings from the database
        optimization_settings = self.data.get('optimization_settings', [])
        if optimization_settings:
            # Sort by created_at in descending order to get the latest
            latest_settings = sorted(
                optimization_settings, 
                key=lambda x: x.get('created_at', datetime.datetime.min), 
                reverse=True
            )[0]
            
            # Update scheduler parameters from database settings
            if self.population_size is None:
                self.population_size = latest_settings.get('population_size', 50)
            if self.generations is None:
                self.generations = latest_settings.get('generations', 30)
            
            # Store the buffer time from settings
            self.buffer_time_hours = latest_settings.get('buffer_time_hours', 0)
            
            # Use start_date from settings if not provided
            if start_date is None and 'start_date' in latest_settings:
                start_date = latest_settings.get('start_date')
            
            # Store the entire settings object
            self.settings = latest_settings
        else:
            # Fallback to default values if no settings found
            if self.population_size is None:
                self.population_size = 50
            if self.generations is None:
                self.generations = 30
            self.buffer_time_hours = 0
        
        # Set default start date if not provided
        if start_date is None:
            # Start at the beginning of the next day at 6 AM (typical shift start)
            now = datetime.datetime.now()
            self.start_date = (now + timedelta(days=1)).replace(
                hour=6, minute=0, second=0, microsecond=0
            )
        else:
            # If start date is provided, ensure it starts at 6 AM
            if isinstance(start_date, datetime.date):
                start_date = datetime.datetime.combine(start_date, datetime.time(6, 0))
            self.start_date = start_date.replace(hour=6, minute=0, second=0, microsecond=0)
        
        # Get prepared data and check for success
        prepared_data = prepare_data_for_optimization(
            self.data, 
            start_date=self.start_date, 
            planning_horizon_days=planning_horizon_days
        )
        
        if prepared_data["status"] != "success":
            print(f"Error preparing data: {prepared_data['message']}")
            return False
            
        # Store the prepared data
        self.scheduling_data = prepared_data["data"]
        
        # Store the planning horizon end date
        self.planning_horizon_end = self.start_date + timedelta(days=planning_horizon_days)
        
        # Extract orders from the data
        self.orders = self.data.get('manufacturing_orders', [])
        
        # Get completed operations
        completed_operations = self.data.get('completed_operations', [])
        
        # Create a mapping of completed quantities for each manufacturing order and operation
        completed_quantities = {}
        for completed_op in completed_operations:
            mo_id = completed_op.get('manufacturing_order_id')
            op_id = completed_op.get('operation_id')
            quantity = completed_op.get('quantity', 0)
            
            if mo_id and op_id:
                if mo_id not in completed_quantities:
                    completed_quantities[mo_id] = {}
                if op_id not in completed_quantities[mo_id]:
                    completed_quantities[mo_id][op_id] = 0
                completed_quantities[mo_id][op_id] += quantity
        
        # Group operations by product for easier access and calculate remaining quantities
        self.operations_by_product = {}
        product_operations = self.data.get('product_operations_with_details', [])
        
        for op in product_operations:
            product_id = op['product_id']
            if product_id not in self.operations_by_product:
                self.operations_by_product[product_id] = []
            
            # Create a copy of the operation to modify
            operation = dict(op)
            
            # Find any manufacturing orders that use this product and operation
            for order in self.orders:
                if order['product_id'] == product_id:
                    mo_id = order['id']
                    op_id = operation['operation_id']
                    planned_quantity = order.get('planned_quantity', 0)
                    
                    # Calculate completed quantity for this operation in this order
                    completed_qty = completed_quantities.get(mo_id, {}).get(op_id, 0)
                    
                    # Calculate remaining quantity
                    remaining_qty = max(0, planned_quantity - completed_qty)
                    
                    # Store both quantities in the operation
                    operation['planned_quantity'] = planned_quantity
                    operation['completed_quantity'] = completed_qty
                    operation['remaining_quantity'] = remaining_qty
                    operation['order_id'] = mo_id
            
            self.operations_by_product[product_id].append(operation)
        
        # Sort operations by sequence number for each product
        for product_id, operations in self.operations_by_product.items():
            self.operations_by_product[product_id] = sorted(operations, key=lambda op: op['sequence_number'])
        
        # Initialize resource usage with tasks from previous run or unassigned tasks
        if previous_run_id:
            tasks_result = fetch_tasks_for_run(previous_run_id)
        else:
            tasks_result = fetch_unassigned_tasks()
        
        if tasks_result["status"] == "success":
            tasks = tasks_result["data"]
            
            # Initialize the resource usage dictionary
            self.resource_usage = {
                'machines': {},
                'workers': {},
                'complete_tasks': []  # Store complete task information
            }
            
            def convert_to_naive_utc(dt):
                """Helper function to convert datetime to naive UTC"""
                if dt is None:
                    return None
                # If it's a string, parse it first
                if isinstance(dt, str):
                    # Remove 'Z' and try to parse as ISO format
                    dt = dt.replace('Z', '+00:00')
                    try:
                        dt = datetime.datetime.fromisoformat(dt)
                    except ValueError:
                        try:
                            # Fallback for other string formats
                            dt = datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S%z')
                        except ValueError:
                            return None
                
                # If it's already a datetime
                if isinstance(dt, datetime.datetime):
                    # If it's aware (has tzinfo), convert to UTC and make naive
                    if dt.tzinfo is not None:
                        dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
                    return dt
                return None
            
            # Process each task
            for task in tasks:
                machine_id = task.get('machine_id')
                worker_id = task.get('worker_id')
                start_time = convert_to_naive_utc(task.get('start_time'))
                end_time = convert_to_naive_utc(task.get('end_time'))
                operation_uuid = task.get('operation_id')
                order_id = task.get('manufacturing_order_id')
                task_type = task.get('type', 'operation')
                
                # Skip tasks without necessary data
                if not start_time or not end_time:
                    continue
                
                # Store complete task information
                self.resource_usage['complete_tasks'].append(task)
                
                # Add to machine usage if a machine is assigned
                if machine_id:
                    if machine_id not in self.resource_usage['machines']:
                        self.resource_usage['machines'][machine_id] = []
                    
                    self.resource_usage['machines'][machine_id].append({
                        'start_time': start_time,
                        'end_time': end_time,
                        'operation_id': operation_uuid,
                        'order_id': order_id,
                        'type': task_type
                    })
                
                # Add to worker usage if a worker is assigned
                if worker_id:
                    if worker_id not in self.resource_usage['workers']:
                        self.resource_usage['workers'][worker_id] = []
                    
                    self.resource_usage['workers'][worker_id].append({
                        'start_time': start_time,
                        'end_time': end_time,
                        'operation_id': operation_uuid,
                        'order_id': order_id,
                        'type': task_type
                    })
        
            # Sort the usage periods for each resource
            for machine_id in self.resource_usage['machines']:
                self.resource_usage['machines'][machine_id].sort(key=lambda x: x['start_time'])
            
            for worker_id in self.resource_usage['workers']:
                self.resource_usage['workers'][worker_id].sort(key=lambda x: x['start_time'])
        else:
            print(f"Warning: Failed to fetch tasks: {tasks_result['message']}")
            # Initialize empty resource usage if we couldn't fetch tasks
            self.resource_usage = {
                'machines': {},
                'workers': {},
                'complete_tasks': []
            }
        
        return True

    def initialize_population(self) -> List[Dict]:
        """
        Initialize a population of random individuals.
        
        Each individual is a dictionary with:
        - order_sequence: A list of order indices indicating scheduling priority
        - operation_splits: A dictionary mapping operation_ids to number of splits
        
        Returns:
            List of individuals
        """
        population = []
        
        # Build set of all operations across all products
        all_operations = set()
        for product_ops in self.operations_by_product.values():
            for op in product_ops:
                all_operations.add(str(op['id']))
        
        # First individual: maximum splits for all operations
        order_sequence = list(range(len(self.orders)))
        random.shuffle(order_sequence)
        
        operation_splits = {}
        for op_id in all_operations:
            # Set maximum splits (12) for all operations
            operation_splits[op_id] = 12
        
        individual = {
            'order_sequence': order_sequence,
            'operation_splits': operation_splits
        }
        
        population.append(individual)
        
        # Rest of population with random splits
        for _ in range(self.population_size - 1):
            # Create a random order sequence
            order_sequence = list(range(len(self.orders)))
            random.shuffle(order_sequence)
            
            # Create random operation splits
            operation_splits = {}
            for op_id in all_operations:
                # Randomly choose between 1-12 splits
                operation_splits[op_id] = random.randint(1, 12)
            
            individual = {
                'order_sequence': order_sequence,
                'operation_splits': operation_splits
            }
            
            population.append(individual)
        
        return population

    def find_next_available_slot(self, resource_type, resource_id, start_time, duration_hours, resource_usage):
        """
        Find the next available time slot for a resource that can accommodate the required duration.
        Takes into account both the resource's availability windows and current usage.
        
        Args:
            resource_type (str): Type of resource ('machines' or 'workers')
            resource_id (UUID): ID of the resource
            start_time (datetime): Earliest time to start looking from
            duration_hours (float): Required duration in hours
            resource_usage (dict): Current resource usage tracking
            
        Returns:
            tuple: (start_time, end_time) for the next available slot, or (None, None) if no slot found
        """
        try:
            # If resource not tracked yet, initialize it
            if resource_type not in resource_usage:
                resource_usage[resource_type] = {}
            if resource_id not in resource_usage[resource_type]:
                resource_usage[resource_type][resource_id] = []
            
            # Get all usage periods for this resource
            usage_periods = sorted(
                resource_usage[resource_type][resource_id],
                key=lambda x: x['start_time']
            )
            
            # If no usage yet, return the start time
            if not usage_periods:
                return start_time, start_time + timedelta(hours=duration_hours)
            
            # Find gaps between usage periods that can fit the duration
            current_time = start_time
            
            # Check if we can fit before the first usage
            first_usage = usage_periods[0]
            if current_time + timedelta(hours=duration_hours) <= first_usage['start_time']:
                return current_time, current_time + timedelta(hours=duration_hours)
            
            # Check gaps between usages
            for i in range(len(usage_periods) - 1):
                current_usage = usage_periods[i]
                next_usage = usage_periods[i + 1]
                
                gap_start = current_usage['end_time']
                gap_end = next_usage['start_time']
                
                # Start looking from the maximum of gap_start and our desired start time
                effective_start = max(gap_start, current_time)
                
                # If we can fit the duration in this gap, return it
                if effective_start + timedelta(hours=duration_hours) <= gap_end:
                    return effective_start, effective_start + timedelta(hours=duration_hours)
            
            # Check if we can fit after the last usage
            last_usage = usage_periods[-1]
            current_time = max(last_usage['end_time'], current_time)
            return current_time, current_time + timedelta(hours=duration_hours)
            
        except Exception as e:
            # If there's any exception, initialize the resource and return a default slot
            print(f"Exception in find_next_available_slot for {resource_type} {resource_id}: {str(e)}") # Keep critical exception prints
            if resource_type not in resource_usage:
                resource_usage[resource_type] = {}
            resource_usage[resource_type][resource_id] = []
            return start_time, start_time + timedelta(hours=duration_hours)

    def schedule_within_availability(self, operation_uuid, quantity, op, earliest_start_time, resource_schedules, resource_usage):
        """
        Schedule an operation respecting both resource availability windows and current resource usage.
        Also updates the passed resource_usage dictionary.
        """
        schedule_entries = []
        remaining_quantity = quantity
        current_start = round_to_interval(earliest_start_time)  # Round start time to nearest interval
        duration_hours = round_duration_to_interval(op['duration'] / 60)  # Round duration to nearest interval
        order_id = op.get('order_id')
        worker_required = self.scheduling_data['capability_mappings']['operation_requirements'][operation_uuid]['worker_required']
        total_time_needed = duration_hours * quantity
        if current_start + timedelta(hours=total_time_needed) > self.planning_horizon_end:
            return []
        while remaining_quantity > 0:
            operation_resources = self.scheduling_data['capability_mappings']['operation_resources'].get(operation_uuid, {})
            machine_ids = operation_resources.get('machines', [])
            if worker_required == "SetupOnly":
                worker_ids = operation_resources.get('setup_workers', [])
            elif worker_required == "OperationOnly":
                worker_ids = operation_resources.get('operation_workers', [])
            elif worker_required == "SetupAndOperation":
                worker_ids = operation_resources.get('setup_and_operation_workers', [])
            else:
                worker_ids = []
            best_slot = None
            best_resources = None
            for machine_id in (machine_ids or [None]):
                machine_intervals = [(current_start, self.planning_horizon_end)] if not machine_id else get_resource_available_intervals('machines', machine_id, current_start, duration_hours, resource_usage, resource_schedules, self.planning_horizon_end)
                for worker_id in (worker_ids or [None]):
                    worker_intervals = [(current_start, self.planning_horizon_end)] if not worker_id else get_resource_available_intervals('workers', worker_id, current_start, duration_hours, resource_usage, resource_schedules, self.planning_horizon_end)
                    i, j = 0, 0
                    while i < len(machine_intervals) and j < len(worker_intervals):
                        m_start, m_end = machine_intervals[i]
                        w_start, w_end = worker_intervals[j]
                        overlap_start = round_to_interval(max(m_start, w_start))  # Round overlap start to interval
                        overlap_end = min(m_end, w_end)
                        if overlap_end <= overlap_start:
                            if m_end < w_end:
                                i += 1
                            else:
                                j += 1
                            continue
                        setup_time_mins = 0
                        if machine_id:
                            setup_time_mins = calculate_setup_time(
                                operation_uuid,
                                machine_id,
                                overlap_start,
                                op.get('setup_time', 0),
                                resource_usage,
                                order_id
                            )
                        setup_hours = round_duration_to_interval(setup_time_mins / 60)  # Round setup time to interval
                        processing_start = round_to_interval(overlap_start + timedelta(hours=setup_hours))  # Round processing start to interval
                        available_time = (overlap_end - processing_start).total_seconds() / 3600
                        processable_quantity = min(remaining_quantity, available_time / duration_hours)
                        if processable_quantity > 0:
                            processing_time = duration_hours * processable_quantity
                            processing_end = round_to_interval(processing_start + timedelta(hours=processing_time))  # Round processing end to interval
                            if not best_slot or overlap_start < best_slot:
                                best_slot = overlap_start
                                best_resources = {
                                    'machine_id': machine_id,
                                    'worker_id': worker_id,
                                    'processing_start': processing_start,
                                    'processing_end': processing_end,
                                    'setup_time_mins': setup_time_mins,
                                    'processable_quantity': processable_quantity
                                }
                        if m_end < w_end:
                            i += 1
                        else:
                            j += 1
            if not best_slot or not best_resources:
                break
            # Update resource usage
            if worker_required == "SetupOnly":
                # Only require worker for setup period
                if best_resources['setup_time_mins'] > 0:
                    resource_usage = update_resource_usage(
                        resource_usage,
                        operation_uuid,
                        best_resources['machine_id'],
                        best_resources['worker_id'] if worker_required in ["SetupOnly", "SetupAndOperation"] else None,
                        best_slot,
                        best_resources['processing_start'],
                        'setup',
                        order_id
                    )
                resource_usage = update_resource_usage(
                    resource_usage,
                    operation_uuid,
                    best_resources['machine_id'],
                    best_resources['worker_id'] if worker_required in ["OperationOnly", "SetupAndOperation"] else None,
                    best_resources['processing_start'],
                    best_resources['processing_end'],
                    'operation',
                    order_id
                )
            else:
                if best_resources['setup_time_mins'] > 0:
                    resource_usage = update_resource_usage(
                        resource_usage,
                        operation_uuid,
                        best_resources['machine_id'],
                        best_resources['worker_id'] if worker_required in ["SetupOnly", "SetupAndOperation"] else None,
                        best_slot,
                        best_resources['processing_start'],
                        'setup',
                        order_id
                    )
                resource_usage = update_resource_usage(
                    resource_usage,
                    operation_uuid,
                    best_resources['machine_id'],
                    best_resources['worker_id'] if worker_required in ["OperationOnly", "SetupAndOperation"] else None,
                    best_resources['processing_start'],
                    best_resources['processing_end'],
                    'operation',
                    order_id
                )
            schedule_entry = {
                'start_time': best_slot,
                'end_time': best_resources['processing_end'],
                'machine_id': best_resources['machine_id'],
                'worker_id': best_resources['worker_id'],
                'quantity': best_resources['processable_quantity'],
                'worker_required': worker_required,
                'setup_time_mins': best_resources['setup_time_mins'],
                'total_duration_mins': (op.get('duration', 0) * best_resources['processable_quantity']) + best_resources['setup_time_mins'],
                'required_date': op.get('required_date')
            }
            schedule_entries.append(schedule_entry)
            remaining_quantity -= best_resources['processable_quantity']
            current_start = best_resources['processing_end']
        return schedule_entries

    def evaluate_population(self, population: List[Dict]) -> List[Dict]:
        """
        Evaluate all individuals in a population using parallel processing across multiple cores.
        
        Args:
            population: List of individuals to evaluate
            
        Returns:
            List of evaluated individuals with fitness scores and schedules
        """
        evaluated_population = []
        
        # Prepare the shared data needed by all worker processes
        shared_context = {
            'start_date': self.start_date,
            'data': self.data,
            'scheduling_data': self.scheduling_data,
            'orders': self.orders,
            'operations_by_product': self.operations_by_product,
            'planning_horizon_end': self.planning_horizon_end,
            'buffer_time_hours': self.buffer_time_hours,
            'initial_resource_usage': self.resource_usage  # Add the pre-initialized resource usage
        }
        
        # Define partial function with shared context
        eval_func = partial(
            evaluate_individual_worker,
            start_date=shared_context['start_date'],
            data=shared_context['data'],
            scheduling_data=shared_context['scheduling_data'],
            orders=shared_context['orders'],
            operations_by_product=shared_context['operations_by_product'],
            planning_horizon_end=shared_context['planning_horizon_end'],
            buffer_time_hours=shared_context['buffer_time_hours'],
            initial_resource_usage=shared_context['initial_resource_usage']  # Pass the pre-initialized resource usage
        )
        
        # Use ProcessPoolExecutor for true parallel execution across CPU cores
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            evaluations = list(executor.map(eval_func, population))
        
        # Combine original individuals with their evaluations
        for i, individual in enumerate(population):
            evaluation = evaluations[i]
            evaluated_individual = {
                **individual,
                'evaluation': evaluation
            }
            evaluated_population.append(evaluated_individual)
        
        return evaluated_population

    def selection(self, evaluated_population: List[Dict]) -> List[Dict]:
        """
        Select individuals for reproduction using tournament selection.
        
        Args:
            evaluated_population: List of evaluated individuals
            
        Returns:
            List of selected individuals
        """
        # Sort population by fitness (lower is better)
        sorted_population = sorted(
            evaluated_population,
            key=lambda x: x['evaluation']['fitness']
        )
        
        # Select the elite individuals
        elite = sorted_population[:self.elite_size]
        
        # Select additional individuals using tournament selection
        num_to_select = int(self.population_size * self.selection_percentage)
        selected = elite.copy()
        
        while len(selected) < num_to_select:
            # Tournament selection
            tournament_size = 3
            tournament = random.sample(evaluated_population, tournament_size)
            
            # Find the best individual in the tournament
            best = min(tournament, key=lambda x: x['evaluation']['fitness'])
            
            # Add to selected population
            selected.append(best)
        
        return selected

    def crossover(self, parent1: Dict, parent2: Dict) -> Tuple[Dict, Dict]:
        """
        Create two offspring by crossing over two parents.
        
        Args:
            parent1: First parent individual
            parent2: Second parent individual
            
        Returns:
            Tuple of two offspring individuals
        """
        # Check if crossover should occur
        if random.random() > self.crossover_rate:
            return copy.deepcopy(parent1), copy.deepcopy(parent2)
        
        child1 = copy.deepcopy(parent1)
        child2 = copy.deepcopy(parent2)
        
        # Crossover order sequence using ordered crossover (OX)
        order_len = len(parent1['order_sequence'])
        if order_len >= 3:
            # Select crossover points
            cx_point1 = random.randint(0, order_len - 2)
            cx_point2 = random.randint(cx_point1 + 1, order_len - 1)
            
            # Create child 1
            middle1 = parent1['order_sequence'][cx_point1:cx_point2]
            remaining1 = [item for item in parent2['order_sequence'] if item not in middle1]
            child1['order_sequence'] = remaining1[:cx_point1] + middle1 + remaining1[cx_point1:]
            
            # Create child 2
            middle2 = parent2['order_sequence'][cx_point1:cx_point2]
            remaining2 = [item for item in parent1['order_sequence'] if item not in middle2]
            child2['order_sequence'] = remaining2[:cx_point1] + middle2 + remaining2[cx_point1:]
        
        # Crossover operation splits using uniform crossover
        for op_id in parent1['operation_splits'].keys():
            if random.random() < 0.5:
                # Swap splits between children
                child1['operation_splits'][op_id] = parent2['operation_splits'][op_id]
                child2['operation_splits'][op_id] = parent1['operation_splits'][op_id]
        
        return child1, child2

    def mutation(self, individual: Dict) -> Dict:
        """
        Mutate an individual by randomly changing its genes.
        
        Args:
            individual: The individual to mutate
            
        Returns:
            The mutated individual
        """
        # Check if mutation should occur
        if random.random() > self.mutation_rate:
            return individual
        
        # Create a mutable copy
        mutated = copy.deepcopy(individual)
        
        # Mutate order sequence - swap mutation
        if len(mutated['order_sequence']) >= 2:
            idx1, idx2 = random.sample(range(len(mutated['order_sequence'])), 2)
            mutated['order_sequence'][idx1], mutated['order_sequence'][idx2] = mutated['order_sequence'][idx2], mutated['order_sequence'][idx1]
        
        # Mutate operation splits
        # Select a random operation to mutate
        if mutated['operation_splits']:
            op_id = random.choice(list(mutated['operation_splits'].keys()))
            current_splits = mutated['operation_splits'][op_id]
            
            # Either increment or decrement the number of splits
            if current_splits == 1:
                mutated['operation_splits'][op_id] = 2
            elif current_splits == 4:
                mutated['operation_splits'][op_id] = 3
            else:
                # For values 2 or 3, randomly increase or decrease
                mutated['operation_splits'][op_id] = current_splits + random.choice([-1, 1])
        
        return mutated

    def breed_population(self, selected: List[Dict]) -> List[Dict]:
        """
        Create a new population by breeding the selected individuals.
        
        Args:
            selected: List of selected individuals
            
        Returns:
            New population of individuals
        """
        # Keep the elite individuals
        sorted_selected = sorted(
            selected,
            key=lambda x: x['evaluation']['fitness']
        )
        elite = [copy.deepcopy(ind) for ind in sorted_selected[:self.elite_size]]
        
        # Create offspring by crossing over and mutating
        offspring = []
        
        # Keep breeding until we have enough offspring
        while len(offspring) + len(elite) < self.population_size:
            # Select two parents
            parent1, parent2 = random.sample(selected, 2)
            
            # Perform crossover
            child1, child2 = self.crossover(parent1, parent2)
            
            # Perform mutation
            child1 = self.mutation(child1)
            child2 = self.mutation(child2)
            
            # Add to offspring
            offspring.append(child1)
            
            # Make sure we don't exceed population size
            if len(offspring) + len(elite) < self.population_size:
                offspring.append(child2)
        
        # Combine elite and offspring to create new population
        new_population = elite + offspring
        
        # Remove evaluation data from new population
        for individual in new_population:
            if 'evaluation' in individual:
                del individual['evaluation']
        
        return new_population

    def run_optimization(self, start_date=None, planning_horizon_days=120, debug_level=1, optimization_run_id=None, previous_run_id=None):
        """
        Run the genetic algorithm optimization process.
        
        Args:
            start_date: Optional start date for planning, defaults to current date
            planning_horizon_days: Number of days to plan for
            debug_level: Level of debug output (0=minimal, 1=normal, 2=verbose)
            optimization_run_id: Optional ID for this optimization run
            previous_run_id: Optional ID of previous optimization run to use as basis
            
        Returns:
            Dict: Best schedule and associated metrics
        """
        # Prepare data
        if not self.prepare_data(start_date, planning_horizon_days, previous_run_id):
            return {
                "status": "error",
                "message": "Failed to prepare data for optimization"
            }
        
        # Set debug level as attribute to control verbosity throughout 
        self.debug_level = debug_level
        
        print(f"Starting genetic algorithm optimization with {self.population_size} individuals for {self.generations} generations")
        print(f"Using {self.max_workers} CPU cores for parallel evaluation")
        
        # Start timing
        start_time = time.time()
        
        # Generate a unique ID for this optimization run if not provided
        if optimization_run_id is None:
            optimization_run_id = uuid.uuid4()
        else:
            # Convert string to UUID if needed
            if isinstance(optimization_run_id, str):
                optimization_run_id = uuid.UUID(optimization_run_id)
                
        print(f"Optimization Run ID: {optimization_run_id}")
        
        # Create the optimization run record in the database
        if not create_optimization_run(optimization_run_id):
            print("Warning: Failed to create optimization run record in the database. Proceeding with optimization.")
        
        # Initialize population
        population = self.initialize_population()
        
        # Track best individual across generations
        best_individual = None
        best_fitness = float('inf')
        
        # Run for specified number of generations
        for generation in range(self.generations):
            gen_start_time = time.time()
            
            print(f"\n===== Generation {generation+1}/{self.generations} =====")
            
            # Evaluate population
            evaluated_population = self.evaluate_population(population)
            
            # Find the best individual in this generation
            generation_best = min(evaluated_population, key=lambda x: x['evaluation']['fitness'])
            generation_best_fitness = generation_best['evaluation']['fitness']
            
            # Update overall best if this generation's best is better
            if best_individual is None or generation_best_fitness < best_fitness:
                best_individual = copy.deepcopy(generation_best)
                best_fitness = generation_best_fitness
            
            # Print progress
            gen_time = time.time() - gen_start_time
            print(f"Generation {generation+1}/{self.generations} - Best fitness: {generation_best_fitness:.2f} - Time: {gen_time:.2f}s")
            
            # Show some metrics
            best_eval = generation_best['evaluation']
            print(f"  Tardiness: {best_eval['tardiness']:.2f}h | On-time orders: {best_eval['on_time_orders']}/{best_eval['total_orders_scheduled']} | Avg Order Makespan: {best_eval['avg_order_makespan']:.2f}h")
            
            # Break early if we're at the last generation
            if generation == self.generations - 1:
                break
            
            # Selection
            selected = self.selection(evaluated_population)
            
            # Breeding
            population = self.breed_population(selected)
        
        # Calculate total time
        total_time = time.time() - start_time
        print(f"\nOptimization complete - Total time: {total_time:.2f}s")
        
        if best_individual:
            # Extract the best schedule and metrics
            best_schedule = best_individual['evaluation']['schedule']
            final_metrics = best_individual['evaluation']
            tasks_count = len(best_schedule)
            
            # Add the calculated total runtime to the metrics dictionary
            final_metrics['computation_time'] = total_time
            
            # Final check for worker availability issues
            print("\nFinal check for worker availability issues in best schedule...")
            worker_issues = self._check_worker_availability_issues(best_schedule)
            final_metrics['worker_availability_issues'] = len(worker_issues)
            if worker_issues:
                print(f"WARNING: Found {len(worker_issues)} operations scheduled outside worker availability in final schedule!")
                # if debug_level >= 1: # Removed debug level check for final warning details
                # Print details of first 10 issues
                for idx, issue in enumerate(worker_issues[:10]):
                    print(f"  Issue {idx+1}: Operation {issue['operation_name']} with worker {issue['worker_id']} at {issue['start_time']}")
            else:
                print("No worker availability issues found in final schedule.")
            
            # Get fetched tasks from resource_usage
            fetched_tasks = self.resource_usage.get('complete_tasks', [])
            if fetched_tasks:
                print(f"Found {len(fetched_tasks)} tasks from previous run to save under new optimization run ID")
            
            # Save the best schedule to the database along with fetched tasks
            save_tasks_result = save_schedule_to_db(best_schedule, optimization_run_id, fetched_tasks)
            print(f"Database save tasks result: {save_tasks_result}")
            
            # Update the optimization run record with completion status and metrics
            update_status = 'completed' if save_tasks_result.get('status') == 'success' else 'completed_with_save_errors'
            
            # Prepare parameters dictionary with JSON-serializable values
            optimization_parameters = {
                'genetic_algorithm': {
                    'population_size': self.population_size,
                    'generations': self.generations,
                    'crossover_rate': float(self.crossover_rate),  # Ensure float
                    'mutation_rate': float(self.mutation_rate),    # Ensure float
                    'selection_percentage': float(self.selection_percentage),  # Ensure float
                    'elite_size': self.elite_size,
                    'max_workers': self.max_workers
                },
                'scheduling': {
                    'buffer_time_hours': float(self.buffer_time_hours),  # Ensure float
                    'start_date': self.start_date.isoformat() if self.start_date else None,  # Convert datetime to ISO string
                    'planning_horizon_end': self.planning_horizon_end.isoformat() if self.planning_horizon_end else None
                },
                'solution': {
                    'order_sequence': [
                        {
                            'index': idx,
                            'order_id': str(self.orders[idx]['id']) if idx < len(self.orders) else None,
                            'order_number': self.orders[idx]['order_number'] if idx < len(self.orders) else None
                        }
                        for idx in best_individual['order_sequence']
                    ],
                    'operation_splits': {
                        str(op_id): int(splits)  # Ensure operation IDs are strings and splits are integers
                        for op_id, splits in best_individual['operation_splits'].items()
                    }
                }
            }
            
            update_optimization_run(
                optimization_run_id, 
                status=update_status, 
                metrics=final_metrics, 
                tasks_count=tasks_count,
                parameters=optimization_parameters
            )

            print("\n===== ORDER STATUS SUMMARY =====")
            print("On-time Orders:")
            for order_id, metrics in best_individual['evaluation']['order_metrics'].items():
                if metrics['is_on_time'] and metrics['end_time'] is not None:
                    print(f"  - Order {order_id}: Completed at {metrics['end_time']} (Required: {metrics['required_date']})")
            
            print("\nLate Orders:")
            for order_id, metrics in best_individual['evaluation']['order_metrics'].items():
                if not metrics['is_on_time'] and metrics['end_time'] is not None:
                    tardiness = metrics['tardiness']
                    print(f"  - Order {order_id}: Completed at {metrics['end_time']} (Required: {metrics['required_date']}, Late by: {tardiness})")

            # Return the best schedule and metrics
            return {
                
                "status": "success",
                "message": "Optimization completed successfully",
                "optimization_run_id": str(optimization_run_id), # Include the run ID
                "schedule": best_schedule,
                "metrics": {
                    "tardiness": best_individual['evaluation']['tardiness'],
                    "on_time_orders": best_individual['evaluation']['on_time_orders'],
                    "total_orders_scheduled": best_individual['evaluation']['total_orders_scheduled'],
                    "avg_order_makespan": best_individual['evaluation']['avg_order_makespan'],
                    "fitness": best_individual['evaluation']['fitness'],
                    "computation_time": total_time,
                    "worker_availability_issues": len(worker_issues)
                },
                "parameters": {
                    "order_sequence": [self.orders[idx]['order_number'] for idx in best_individual['order_sequence'] if idx < len(self.orders)],
                }
            }
        else:
            # Update the optimization run record with failed status
            update_optimization_run(
                optimization_run_id, 
                status='failed', 
                metrics={'computation_time': total_time}, # Provide runtime even on failure
                tasks_count=0
            )
            
            return {
                "status": "error",
                "message": "Failed to find a valid schedule",
                "optimization_run_id": str(optimization_run_id) # Include run ID even on failure
            }

    def _run_genetic_algorithm(self):
        # Implementation of the genetic algorithm logic
        # This method should return the best individual found by the genetic algorithm
        pass

    def _check_worker_availability_issues(self, schedule):
        """
        Check for operations scheduled outside of worker availability windows.
        
        Args:
            schedule: List of scheduled operations
            
        Returns:
            List of operations with worker availability issues
        """
        issues = []
        
        for op in schedule:
            worker_id = op.get('worker_id')
            if not worker_id:
                continue  # Skip operations that don't require workers
                
            start_time = op.get('start_time')
            end_time = op.get('end_time')
            if not start_time or not end_time:
                continue
                
            # Check if operation falls within worker availability window
            worker_available = False
            worker_schedule = self.scheduling_data['resource_schedules']['workers'].get(worker_id, {}).get('availability', [])
            
            for window in worker_schedule:
                if window.get('available') and window.get('start') <= start_time and end_time <= window.get('end'):
                    worker_available = True
                    break
                    
            if not worker_available:
                issues.append({
                    'operation_id': op.get('operation_id'),
                    'operation_name': op.get('operation_name'),
                    'worker_id': worker_id,
                    'start_time': start_time,
                    'end_time': end_time,
                    'order_id': op.get('order_id')
                })
                
        return issues


def main():
    """
    Main function to run the genetic scheduler.
    """
    # Get the number of CPU cores available
    cpu_count = multiprocessing.cpu_count()
    print(f"Available CPU cores: {cpu_count}")
    
    # Create scheduler without specifying population_size and generations
    # These will be loaded from the database in prepare_data
    # Use 90% of available cores to avoid overwhelming the system
    max_workers = max(1, int(cpu_count * 0.9))
    scheduler = GeneticScheduler(
        crossover_rate=0.8,       # Probability of crossover
        mutation_rate=0.2,        # Probability of mutation
        selection_percentage=0.4, # Percentage of population to select
        elite_size=5,             # Number of best individuals to preserve
        max_workers=max_workers   # Use 90% of available CPU cores for parallel processing
    )
    
    # Run optimization with debug level 1 (normal output level)
    result = scheduler.run_optimization(debug_level=1)
    
    if result["status"] == "success":
        # Print summary
        print("\n===== OPTIMIZATION RESULTS =====")
        print(f"Fitness score: {result['metrics']['fitness']:.2f}")
        print(f"Total tardiness: {result['metrics']['tardiness']:.2f} hours")
        print(f"On-time orders: {result['metrics']['on_time_orders']}/{result['metrics']['total_orders_scheduled']}")
        print(f"Average order makespan: {result['metrics']['avg_order_makespan']:.2f} hours")
        print(f"Computation time: {result['metrics']['computation_time']:.2f} seconds")
        print(f"Worker availability issues: {result['metrics']['worker_availability_issues']}")
        
        # Print schedule summary
        schedule = result['schedule']
        print(f"\nScheduled {len(schedule)} operations")
        print(f"Total orders scheduled: {len(set(op['order_id'] for op in schedule))}")
        print(f"Optimization Run ID: {result.get('optimization_run_id')}") # Print the run ID
    else:
        print(f"Optimization failed: {result['message']}")


if __name__ == "__main__":
    main() 