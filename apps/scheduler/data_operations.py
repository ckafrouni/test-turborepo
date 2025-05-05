import os
import psycopg
import psycopg.rows
from dotenv import load_dotenv
import datetime
from datetime import timedelta
from uuid import UUID

# Load environment variables from .env file
load_dotenv()

"""
Data Structures Documentation
============================

This module provides functions to fetch, process, and prepare manufacturing data
for scheduling and optimization. Below is documentation for the key data structures:

1. Raw Data (from fetch_all_data)
------------------------------
Return type: dict with the following structure:
{
    "status": str,          # "success" or "error"
    "message": str,         # Status message
    "data": {               # Raw data dictionary containing tables
        "manufacturing_orders": [   # List of dict, each representing an order
            {
                "id": UUID,                 # Order ID
                "order_number": str,        # Order number (e.g., "MO-FFT2")
                "product_id": UUID,         # Product ID
                "planned_quantity": float,  # Quantity to produce
                "status": str,              # Order status (e.g., "pending")
                "required_date": datetime,  # When the order is due
                "sales_order_number": str,  # Related sales order
                "sales_order_priority": int # Priority (higher = more important)
            },
            ...
        ],
        "product_operations_with_details": [  # List of dict, operations for products
            {
                "id": UUID,                 # Product operation ID
                "product_id": UUID,         # Product ID
                "operation_id": UUID,       # Operation ID
                "sequence_number": int,     # Order of operations
                "operation_name": str,      # Name of operation (e.g., "R04")
                "duration": float,          # Time per unit (hours)
                "setup_time": float,        # Initial setup time (hours)
                "workstation_id": UUID,     # Workstation where operation is performed
                "machine_required": bool,   # Whether a machine is required for this operation (not nullable)
                "worker_required": str      # Type of worker capability required:
                                          # "SetupOnly", "OperationOnly", "SetupAndOperation"
                                          # null if no worker is required
            },
            ...
        ],
        "workers_with_schedules": [          # List of dict, worker details
            {
                "id": UUID,                 # Worker ID
                "name": str,                # Worker name
                "email": str,               # Worker email
                "shift_schedules": [        # List of dict, shift schedule
                    {
                        "schedule_id": UUID,    # Schedule ID
                        "day_of_week": int,     # 0=Mon, 1=Tue, ..., 6=Sun
                        "shift_id": UUID,       # Shift ID
                        "shift_name": str,      # Shift name (e.g., "Morning")
                        "start_time": time,     # Shift start (e.g., "06:00:00")
                        "end_time": time        # Shift end (e.g., "14:00:00")
                    },
                    ...
                ],
                "unavailabilities": [       # List of dict, unavailable periods
                    {
                        "id": UUID,             # Unavailability ID
                        "worker_id": UUID,      # Worker ID
                        "start_date": date,     # Start date
                        "end_date": date,       # End date
                        "type": str,            # Type (e.g., "SICKNESS")
                        "description": str      # Description (can be null)
                    },
                    ...
                ],
                "workstation_capabilities": [  # List of dict, worker abilities
                    {
                        "id": UUID,             # Capability ID
                        "worker_id": UUID,      # Worker ID
                        "workstation_id": UUID, # Workstation ID
                        "capability": str,      # One of: "SetupOnly", "OperationOnly", "SetupAndOperation"
                        "workstation_name": str # Name of workstation
                    },
                    ...
                ]
            },
            ...
        ],
        "machines": [                       # List of dict, machine details
            {
                "id": UUID,                 # Machine ID
                "name": str,                # Machine name
                "workstation_id": UUID,     # Workstation ID machine is assigned to
                "status": str               # Machine status
            },
            ...
        ],
        # Additional tables may be present
    }
}

2. Processed Data (from prepare_data_for_optimization)
---------------------------------------------------
Return type: dict with the following structure:
{
    "raw_data": dict,               # Original raw data (structure as above)
    "scheduling_period": {          # Dict containing scheduling timeframe
        "start_date": datetime,     # Start of scheduling period
        "end_date": datetime,       # End of scheduling period
        "planning_horizon_days": int # Number of days to plan for
    },
    "resource_schedules": {         # Resource availability (structure below)
        "machines": {...},
        "workers": {...}
    },
    "capability_mappings": {        # Resource capabilities (structure below)
        "operation_resources": {...},
        "workstation_operations": {...},
        "operation_workstations": {...}
    }
}

3. Resource Schedules (from construct_resource_schedules)
------------------------------------------------------
Return type: dict with the following structure:
{
    "machines": {                    # Machine availability by ID
        "UUID": {                    # Machine ID as key
            "name": str,             # Machine name
            "availability": [        # List of availability periods
                {
                    "start": datetime,  # Start of availability period
                    "end": datetime,    # End of availability period
                    "available": bool   # Always True for machines (24/7)
                },
                ...
            ]
        },
        ...
    },
    "workers": {                     # Worker availability by ID
        "UUID": {                    # Worker ID as key
            "name": str,             # Worker name
            "availability": [        # List of availability periods
                {
                    "start": datetime,  # Start of shift
                    "end": datetime,    # End of shift
                    "available": bool,  # Always True for valid shifts
                    "shift_id": UUID,   # Shift ID
                    "shift_name": str   # Shift name (e.g., "Morning")
                },
                ...
            ]
        },
        ...
    }
}

4. Capability Mappings (from create_capability_mappings)
-----------------------------------------------------
Return type: dict with the following structure:
{
    "operation_resources": {         # For each operation, resources that can perform it
        "UUID": {                    # Operation ID as key
            "setup_workers": [UUID], # List of worker IDs that can set up
            "operation_workers": [UUID], # List of worker IDs that can operate
            "setup_and_operation_workers": [UUID], # List of worker IDs that can do both
            "machines": [UUID]       # List of machine IDs that can perform operation
        },
        ...
    },
    "workstation_operations": {      # For each workstation, its operations
        "UUID": [UUID]               # Workstation ID -> list of operation IDs
    },
    "operation_workstations": {      # For each operation, valid workstations
        "UUID": [UUID]               # Operation ID -> list of workstation IDs
    },
    "operation_requirements": {      # Requirements for each operation
        "UUID": {                    # Operation ID as key
            "machine_required": bool, # Whether a machine is required (not nullable)
            "worker_required": str,   # Type of worker capability required:
                                     # "SetupOnly", "OperationOnly", "SetupAndOperation"
                                     # null if no worker is required
            "operation_name": str,    # Name of the operation
            "duration": float,        # Duration per unit (hours)
            "setup_time": float       # Setup time required (hours)
        },
        ...
    }
}
"""

# Database connection function
def get_db_connection():
    conn = psycopg.connect(
        host=os.getenv('DB_HOST'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        port=os.getenv('DB_PORT', '5432')
    )
    conn.autocommit = True
    return conn

def fetch_all_data():
    """
    Fetch all necessary data from the database
    Returns a dictionary with all table data
    """
    try:
        # Connect to database
        conn = get_db_connection()
        cursor = conn.cursor(row_factory=psycopg.rows.dict_row)
        
        # Dictionary to store all table data
        all_data = {}
        
        # 1. First fetch pending manufacturing orders
        try:
            # Join with sales_order_lines and sales_orders to get required_date
            order_query = """
                SELECT 
                    mo.*,
                    so.required_date,
                    so.order_number as sales_order_number,
                    so.priority as sales_order_priority,
                    so.status as sales_order_status
                FROM 
                    manufacturing_orders mo
                LEFT JOIN 
                    sales_order_lines sol ON mo.sales_order_line_id = sol.id
                LEFT JOIN 
                    sales_orders so ON sol.sales_order_id = so.id
                WHERE 
                    mo.status = 'pending'
                ORDER BY
                    so.priority DESC, 
                    so.required_date ASC
            """
            cursor.execute(order_query)
            pending_orders = cursor.fetchall()
            all_data['manufacturing_orders'] = pending_orders
            
            # 2. Extract product_ids from pending orders
            product_ids = [order['product_id'] for order in pending_orders if order['product_id'] is not None]
            
            # 3. Fetch product operations with operation details for these products
            if product_ids:
                # Convert list of UUIDs to string format for SQL query
                product_ids_str = ','.join([f"'{pid}'" for pid in product_ids])
                
                # Fetch product operations joined with operation details for products in pending orders
                join_query = f"""
                    SELECT 
                        po.*,
                        o.id as operation_id,
                        o.name as operation_name,
                        o.duration,
                        o.setup_time,
                        o.workstation_id,
                        o.machine_required,
                        o.worker_required
                    FROM 
                        product_operations po
                    JOIN 
                        operations o ON po.operation_id = o.id
                    WHERE 
                        po.product_id IN ({product_ids_str})
                    ORDER BY 
                        po.product_id, po.sequence_number
                """
                cursor.execute(join_query)
                product_operations = cursor.fetchall()
                all_data['product_operations_with_details'] = product_operations
            else:
                all_data['product_operations_with_details'] = []
            
            # 4. Extract required workstation IDs from the product operations
            required_workstation_ids = []
            if 'product_operations_with_details' in all_data and all_data['product_operations_with_details']:
                required_workstation_ids = [op['workstation_id'] for op in all_data['product_operations_with_details'] 
                                          if op['workstation_id'] is not None]
                # Remove duplicates
                required_workstation_ids = list(set(required_workstation_ids))
            
            # 5. Fetch workers with their shift schedules and unavailabilities
            # Only get workers who have capabilities for the required workstations
            workers_with_schedules = []
            
            if required_workstation_ids:
                # Convert list of UUIDs to string format for SQL query
                workstation_ids_str = ','.join([f"'{wid}'" for wid in required_workstation_ids])
                
                # Query to get workers assigned to the required workstations
                worker_query = f"""
                    SELECT DISTINCT w.*
                    FROM workers w
                    JOIN worker_workstation_capabilities wwc ON w.id = wwc.worker_id
                    WHERE wwc.workstation_id IN ({workstation_ids_str})
                    ORDER BY w.name
                """
                cursor.execute(worker_query)
                workers = cursor.fetchall()
                
                # For each worker, fetch their shift schedules and unavailabilities
                for worker in workers:
                    worker_id = worker['id']
                    
                    # Fetch shift schedules with shift details
                    shift_query = """
                        SELECT 
                            wss.id as schedule_id,
                            wss.day_of_week,
                            s.id as shift_id,
                            s.name as shift_name,
                            s.start_time,
                            s.end_time
                        FROM 
                            worker_shift_schedules wss
                        JOIN 
                            shifts s ON wss.shift_id = s.id
                        WHERE 
                            wss.worker_id = %s
                        ORDER BY 
                            wss.day_of_week
                    """
                    cursor.execute(shift_query, (worker_id,))
                    shifts = cursor.fetchall()
                    
                    # Fetch unavailabilities
                    unavail_query = """
                        SELECT * FROM worker_unavailabilities
                        WHERE worker_id = %s
                        ORDER BY start_date
                    """
                    cursor.execute(unavail_query, (worker_id,))
                    unavailabilities = cursor.fetchall()
                    
                    # Fetch workstation capabilities
                    capabilities_query = """
                        SELECT 
                            wwc.*,
                            w.name as workstation_name
                        FROM 
                            worker_workstation_capabilities wwc
                        JOIN 
                            workstations w ON wwc.workstation_id = w.id
                        WHERE 
                            wwc.worker_id = %s
                    """
                    cursor.execute(capabilities_query, (worker_id,))
                    capabilities = cursor.fetchall()
                    
                    # Combine worker with their schedules and unavailabilities
                    worker_with_details = dict(worker)
                    worker_with_details['shift_schedules'] = shifts
                    worker_with_details['unavailabilities'] = unavailabilities
                    worker_with_details['workstation_capabilities'] = capabilities
                    
                    workers_with_schedules.append(worker_with_details)
            
            all_data['workers_with_schedules'] = workers_with_schedules
            
        except psycopg.Error as e:
            all_data['manufacturing_orders'] = {'error': str(e)}
            all_data['product_operations_with_details'] = {'error': str(e)}
            all_data['workers_with_schedules'] = {'error': str(e)}
        
        # Fetch data from all other tables (except those we've already handled)
        other_tables = [
            'machines',
            'optimization_settings',
            'products',
            'worker_workstation_capabilities',
            'workstations',
            'sales_order_lines',
            'completed_operations'  # Add completed_operations table here
        ]
        
        for table in other_tables:
            try:
                cursor.execute(f'SELECT * FROM {table}')
                all_data[table] = cursor.fetchall()
            except psycopg.Error as e:
                all_data[table] = {'error': str(e)}
        
        # Close database connection
        cursor.close()
        conn.close()
        
        return {
            "status": "success",
            "message": "Data fetched successfully",
            "data": all_data
        }
    
    except psycopg.Error as e:
        return {
            "status": "error",
            "message": str(e)
        }

def prepare_data_for_optimization(raw_data, start_date=None, planning_horizon_days=30):
    """
    Process raw data to prepare it for optimization.
    
    Args:
        raw_data (dict): Raw data from fetch_all_data()
        start_date (datetime): Start date for optimization
        planning_horizon_days (int): Number of days to plan ahead
        
    Returns:
        dict: Processed data ready for optimization
    """
    try:
        # Set default start date if not provided
        if start_date is None:
            start_date = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Calculate end date based on planning horizon
        end_date = start_date + timedelta(days=planning_horizon_days)
        
        # Generate resource availability schedules
        resource_availability = generate_resource_availability(raw_data, start_date, end_date)
        
        # Check if resource availability generation failed
        if resource_availability["status"] != "success":
            raise Exception(f"Failed to generate resource availability: {resource_availability['message']}")
        
        # Create capability mappings
        capability_mappings = create_capability_mappings(raw_data)
        
        # Return processed data
        return {
            "status": "success",
            "message": "Data prepared successfully",
            "data": {
                "resource_schedules": resource_availability["data"],
                "capability_mappings": capability_mappings,
                "start_date": start_date,
                "end_date": end_date,
                "raw_data": raw_data
            }
        }
        
    except Exception as e:
        print(f"[ERROR] Failed to prepare data for optimization: {str(e)}")
        return {
            "status": "error",
            "message": f"Failed to prepare data for optimization: {str(e)}",
            "data": None
        }

def create_capability_mappings(data):
    """
    Create mappings between operations, workstations, workers, and machines
    to identify which resources are capable of performing each operation
    
    Args:
        data (dict): Raw data from fetch_all_data()
        
    Returns:
        dict: Dictionary containing capability mappings for scheduling
    """
    try:
        # Initialize capability structures
        mappings = {
            # For each operation, which workers and machines can perform it
            "operation_resources": {},
            
            # Mapping from workstation to operations
            "workstation_operations": {},
            
            # Mapping from operation to workstation
            "operation_workstations": {},
            
            # Resource requirements for operations
            "operation_requirements": {}
        }
        
        # Step 1: Create mapping from workstations to operations and vice versa
        # Also store operation requirements
        product_operations = data.get('product_operations_with_details', [])
        
        for op in product_operations:
            # Convert operation_id to UUID if it's a string
            operation_id = op['operation_id']
            if isinstance(operation_id, str):
                operation_id = UUID(operation_id)
            
            workstation_id = op['workstation_id']
            if isinstance(workstation_id, str) and workstation_id:
                workstation_id = UUID(workstation_id)
            
            # Store operation requirements
            machine_required = op['machine_required']  # Now non-nullable
            worker_required = op.get('worker_required')  # null means no worker required
            
            mappings["operation_requirements"][operation_id] = {
                "machine_required": machine_required,
                "worker_required": worker_required,  # will be null if no worker required
                "operation_name": op.get('operation_name', ''),
                "duration": op.get('duration', 0.0),
                "setup_time": op.get('setup_time', 0.0)
            }
            
            # Skip if no workstation assigned
            if not workstation_id:
                continue
                
            # Add to operation -> workstation mapping
            if operation_id not in mappings["operation_workstations"]:
                mappings["operation_workstations"][operation_id] = []
            
            if workstation_id not in mappings["operation_workstations"][operation_id]:
                mappings["operation_workstations"][operation_id].append(workstation_id)
            
            # Add to workstation -> operation mapping
            if workstation_id not in mappings["workstation_operations"]:
                mappings["workstation_operations"][workstation_id] = []
                
            if operation_id not in mappings["workstation_operations"][workstation_id]:
                mappings["workstation_operations"][workstation_id].append(operation_id)
        
        # Step 2: Create operation resource mappings
        for operation_id in mappings["operation_workstations"]:
            # Initialize operation resources
            mappings["operation_resources"][operation_id] = {
                "setup_workers": [],
                "operation_workers": [],
                "setup_and_operation_workers": [],
                "machines": []
            }
            
            # Get workstations for this operation
            workstation_ids = mappings["operation_workstations"][operation_id]
            
            # Get operation requirements
            requirements = mappings["operation_requirements"].get(operation_id, {})
            worker_required = requirements.get("worker_required")  # null means no worker required
            machine_required = requirements["machine_required"]
            
            # Step 2a: Map workers to operations (only if workers are required)
            if worker_required is not None:  # Only map workers if worker_required is not null
                workers = data.get('workers_with_schedules', [])
                
                for worker in workers:
                    worker_id = worker['id']
                    
                    # Get worker's workstation capabilities
                    capabilities = worker.get('workstation_capabilities', [])
                    
                    for cap in capabilities:
                        workstation_id = cap['workstation_id']
                        if isinstance(workstation_id, str):
                            workstation_id = UUID(workstation_id)
                        
                        # Check if worker can work on this operation's workstation
                        if workstation_id in workstation_ids:
                            capability_type = cap['capability']
                            
                            # Handle SetupAndOperation workers - they can do everything
                            if capability_type == "SetupAndOperation":
                                # Add to all lists since they can do everything
                                if worker_id not in mappings["operation_resources"][operation_id]["setup_and_operation_workers"]:
                                    mappings["operation_resources"][operation_id]["setup_and_operation_workers"].append(worker_id)
                                if worker_id not in mappings["operation_resources"][operation_id]["setup_workers"]:
                                    mappings["operation_resources"][operation_id]["setup_workers"].append(worker_id)
                                if worker_id not in mappings["operation_resources"][operation_id]["operation_workers"]:
                                    mappings["operation_resources"][operation_id]["operation_workers"].append(worker_id)
                            
                            # Handle SetupOnly workers
                            elif capability_type == "SetupOnly":
                                if worker_id not in mappings["operation_resources"][operation_id]["setup_workers"]:
                                    mappings["operation_resources"][operation_id]["setup_workers"].append(worker_id)
                            
                            # Handle OperationOnly workers
                            elif capability_type == "OperationOnly":
                                if worker_id not in mappings["operation_resources"][operation_id]["operation_workers"]:
                                    mappings["operation_resources"][operation_id]["operation_workers"].append(worker_id)
            
            # Step 2b: Map machines to operations (only if machines are required)
            if machine_required:  # Now we can use it directly since it's non-nullable
                machines = data.get('machines', [])
                
                for machine in machines:
                    machine_id = machine['id']
                    workstation_id = machine.get('workstation_id')
                    if isinstance(workstation_id, str):
                        workstation_id = UUID(workstation_id)
                    
                    # Check if machine is assigned to one of this operation's workstations
                    if workstation_id in workstation_ids and machine_id not in mappings["operation_resources"][operation_id]["machines"]:
                        mappings["operation_resources"][operation_id]["machines"].append(machine_id)
        
        return mappings
        
    except Exception as e:
        return {}

def construct_resource_schedules(data, start_date, end_date):
    """
    Construct availability schedules for resources (workers and machines)
    
    Args:
        data (dict): Dictionary containing all data from fetch_all_data()
        start_date (datetime): Start date for the scheduling period
        end_date (datetime): End date for the scheduling period
        
    Returns:
        dict: Dictionary with resource schedules
    """
    resource_schedules = {
        'machines': {},
        'workers': {}
    }
    
    # Create 24/7 availability for machines using shift-like structure
    machines = data.get('machines', [])
    for machine in machines:
        machine_id = machine['id']
        resource_schedules['machines'][machine_id] = {
            'name': machine['name'],
            'availability': [{
                'start': start_date,
                'end': end_date,
                'available': True,
                'shift_id': None,  # No shift ID for 24/7 availability
                'shift_name': '24/7'  # Indicate continuous availability
            }]
        }
    
    # Create worker schedules based on shifts and unavailabilities
    workers_with_schedules = data.get('workers_with_schedules', [])
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    
    for worker in workers_with_schedules:
        worker_id = worker['id']
        resource_schedules['workers'][worker_id] = {
            'name': worker['name'],
            'availability': []
        }
        
        # Get worker shift schedules
        shift_schedules = worker.get('shift_schedules', [])
        
        # Get worker unavailabilities
        unavailabilities = worker.get('unavailabilities', [])
        unavailable_periods = []
        
        for unavail in unavailabilities:
            # Convert date strings or date objects to datetime objects
            if isinstance(unavail['start_date'], str):
                unavail_start = datetime.datetime.fromisoformat(unavail['start_date'].split(' ')[0]).replace(hour=0, minute=0, second=0)
            elif isinstance(unavail['start_date'], datetime.date):
                unavail_start = datetime.datetime.combine(unavail['start_date'], datetime.time.min)
            else:
                unavail_start = unavail['start_date']  # Already a datetime
            
            if isinstance(unavail['end_date'], str):
                unavail_end = datetime.datetime.fromisoformat(unavail['end_date'].split(' ')[0]).replace(hour=23, minute=59, second=59)
            elif isinstance(unavail['end_date'], datetime.date):
                unavail_end = datetime.datetime.combine(unavail['end_date'], datetime.time.max)
            else:
                unavail_end = unavail['end_date']  # Already a datetime
                
            unavailable_periods.append((unavail_start, unavail_end))
        
        # Generate day-by-day schedule for the entire period
        current_date = start_date
        while current_date <= end_date:
            # Get day of week (0 = Monday, 6 = Sunday)
            day_of_week = current_date.weekday()
            
            # Check if worker has shifts on this day
            day_shifts = [shift for shift in shift_schedules if shift['day_of_week'] == day_of_week]
            
            # Check if worker is unavailable on this day
            is_unavailable = False
            current_date_start = current_date.replace(hour=0, minute=0, second=0)
            current_date_end = current_date.replace(hour=23, minute=59, second=59)
            
            for unavail_start, unavail_end in unavailable_periods:
                # Check if there's any overlap between unavailability period and current day
                if (unavail_start <= current_date_end) and (unavail_end >= current_date_start):
                    is_unavailable = True
                    break
            
            # If worker has shifts and is not unavailable, add availability periods
            if day_shifts and not is_unavailable:
                for shift in day_shifts:
                    # Parse shift times
                    start_time_str = shift.get('start_time')
                    end_time_str = shift.get('end_time')
                    
                    # Skip shifts without valid times
                    if start_time_str is None or end_time_str is None:
                        continue
                    
                    # Handle time format
                    if isinstance(start_time_str, datetime.time):
                        start_time = start_time_str
                        end_time = end_time_str
                    else:
                        try:
                            # Parse the time if it's a string
                            start_time = datetime.datetime.strptime(str(start_time_str), '%H:%M:%S').time()
                            end_time = datetime.datetime.strptime(str(end_time_str), '%H:%M:%S').time()
                        except Exception:
                            continue  # Skip this shift if we can't parse the time
                    
                    # Create datetime objects for start and end of shift
                    shift_start = datetime.datetime.combine(
                        current_date.date(),
                        start_time
                    )
                    shift_end = datetime.datetime.combine(
                        current_date.date(),
                        end_time
                    )
                    
                    # Handle overnight shifts
                    if shift_end < shift_start:
                        shift_end = shift_end + timedelta(days=1)
                    
                    # Add availability period
                    availability = {
                        'start': shift_start,
                        'end': shift_end,
                        'available': True,
                        'shift_id': shift['shift_id'],
                        'shift_name': shift['shift_name']
                    }
                    resource_schedules['workers'][worker_id]['availability'].append(availability)
            
            # Move to next day
            current_date += timedelta(days=1)
    
    return resource_schedules

def generate_resource_availability(data=None, start_date=None, end_date=None):
    """
    Generate availability schedules for resources (workers and machines)
    for a specified scheduling period
    
    Args:
        data (dict): Data from fetch_all_data(). If None, the function will fetch it.
        start_date (datetime): Start date for the scheduling period. If None, defaults to today.
        end_date (datetime): End date for the scheduling period. If None, defaults to 14 days from start_date.
        
    Returns:
        dict: Dictionary with resource schedules
    """
    try:
        # Fetch data if not provided
        if data is None:
            data_result = fetch_all_data()
            
            if data_result["status"] != "success":
                return {
                    "status": "error",
                    "message": f"Failed to fetch data: {data_result['message']}"
                }
            
            data = data_result["data"]
        
        # Use provided scheduling period or set defaults
        if start_date is None:
            start_date = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        if end_date is None:
            end_date = start_date + timedelta(days=14)
        
        # Construct resource schedules
        resource_schedules = construct_resource_schedules(data, start_date, end_date)
        
        return {
            "status": "success",
            "message": "Resource availability schedules generated successfully",
            "data": resource_schedules
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to generate resource schedules: {str(e)}"
        }

def fetch_tasks_for_run(optimization_run_id):
    """
    Fetch all tasks associated with a specific optimization run ID or null run ID.
    """
    try:
        # Connect to database
        conn = get_db_connection()
        cursor = conn.cursor(row_factory=psycopg.rows.dict_row)
        
        # Query to fetch tasks for the given optimization run or null run ID
        query = """
            SELECT 
                task_id,
                type,
                manufacturing_order_id,
                product_id,
                operation_id,
                machine_id,
                worker_id,
                quantity,
                sequence,
                start_time,
                end_time,
                required_date,
                operation_name,
                setup_time_mins,
                total_duration_mins,
                optimization_run_id
            FROM 
                tasks
            WHERE 
                optimization_run_id = %s OR optimization_run_id IS NULL
            ORDER BY 
                sequence, start_time
        """
        
        # Execute query with the optimization run ID
        cursor.execute(query, (optimization_run_id,))
        tasks = cursor.fetchall()
        
        # Close database connection
        cursor.close()
        conn.close()
        
        return {
            "status": "success",
            "message": "Tasks fetched successfully",
            "data": tasks
        }
    
    except psycopg.Error as e:
        return {
            "status": "error",
            "message": f"Failed to fetch tasks: {str(e)}"
        }

def fetch_unassigned_tasks():
    """
    Fetch all tasks that are not associated with any optimization run (optimization_run_id is NULL).
    
    Returns:
        dict: Dictionary containing status, message, and task data with the following structure:
        {
            "status": str,          # "success" or "error"
            "message": str,         # Status message
            "data": [               # List of task dictionaries
                {
                    "task_id": UUID,
                    "type": str,    # Task type (e.g., "operation")
                    "manufacturing_order_id": UUID,
                    "product_id": UUID,
                    "operation_id": UUID,
                    "machine_id": UUID,
                    "worker_id": UUID,
                    "quantity": int,
                    "sequence": int,
                    "start_time": datetime,
                    "end_time": datetime,
                    "required_date": datetime,
                    "operation_name": str,
                    "setup_time_mins": int,
                    "total_duration_mins": float
                },
                ...
            ]
        }
    """
    try:
        # Connect to database
        conn = get_db_connection()
        cursor = conn.cursor(row_factory=psycopg.rows.dict_row)
        
        # Query to fetch tasks with null optimization_run_id
        query = """
            SELECT 
                task_id,
                type,
                manufacturing_order_id,
                product_id,
                operation_id,
                machine_id,
                worker_id,
                quantity,
                sequence,
                start_time,
                end_time,
                required_date,
                operation_name,
                setup_time_mins,
                total_duration_mins,
                optimization_run_id
            FROM 
                tasks
            WHERE 
                optimization_run_id IS NULL
            ORDER BY 
                sequence, start_time
        """
        
        # Execute query
        cursor.execute(query)
        tasks = cursor.fetchall()
        
        # Close database connection
        cursor.close()
        conn.close()
        
        return {
            "status": "success",
            "message": "Unassigned tasks fetched successfully",
            "data": tasks
        }
    
    except psycopg.Error as e:
        return {
            "status": "error",
            "message": f"Failed to fetch unassigned tasks: {str(e)}"
        } 