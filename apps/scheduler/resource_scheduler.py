"""
Resource Scheduling Module
=========================

This module is responsible for creating and managing resource availability windows for manufacturing resources
(workers and machines). It provides functionality to determine when resources are available for scheduling operations.

Key responsibilities:
- Creates availability windows for resources based on their schedules
- Determines when resources are available for operations
- Handles both worker and machine availability periods
- Considers worker capabilities for different operation types
- Rounds operation times to configurable intervals (default 30 minutes)

The module does NOT:
- Actually schedule the operations (this is done by the genetic scheduler)
- Track resource usage during operations
- Modify or update availability windows during scheduling

Data Structures
--------------
Resource Schedules:
    Dict[str, Dict[str, Any]] = {
        'machines': {
            'UUID': {
                'name': str,
                'availability': List[Dict[str, Any]] = [
                    {
                        'start': datetime,  # Start of availability window
                        'end': datetime,    # End of availability window
                        'available': bool   # Whether resource is available in this window
                    },
                    ...
                ]
            },
            ...
        },
        'workers': {
            'UUID': {
                'name': str,
                'availability': List[Dict[str, Any]] = [
                    {
                        'start': datetime,    # Start of availability window
                        'end': datetime,      # End of availability window
                        'available': bool,    # Whether worker is available in this window
                        'shift_id': UUID,     # ID of the associated shift
                        'shift_name': str     # Name of the shift
                    },
                    ...
                ]
            },
            ...
        }
    }

Capability Mappings:
    Dict[str, Dict[str, Any]] = {
        'operation_resources': {
            UUID: {  # Operation ID as UUID object
                'setup_workers': List[UUID],      # Workers who can perform setup
                'operation_workers': List[UUID],   # Workers who can perform operation
                'setup_and_operation_workers': List[UUID],  # Workers who can do both
                'machines': List[UUID]            # Machines that can perform operation
            },
            ...
        },
        'operation_requirements': {
            UUID: {  # Operation ID as UUID object
                'machine_required': bool,         # Whether operation needs a machine
                'worker_required': Optional[str], # Type of worker needed
                'operation_name': str,            # Name of the operation
                'duration': float,                # Operation duration in hours
                'setup_time': float              # Setup time in hours
            },
            ...
        }
    }
"""

import datetime
from datetime import timedelta
from typing import Dict, Optional, Any, Union
from uuid import UUID
from setup_time_calculator import calculate_setup_time, update_resource_usage

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

def get_resource_available_intervals(resource_type, resource_id, start_time, duration_hours, resource_usage, resource_schedules, planning_horizon_end):
    """
    Returns a list of (start, end) intervals where the resource is available (calendar + usage),
    starting from start_time up to planning_horizon_end.
    """
    calendar_windows = resource_schedules[resource_type].get(resource_id, {}).get('availability', [])
    usage_periods = sorted(resource_usage.get(resource_type, {}).get(resource_id, []), key=lambda x: x['start_time'])
    intervals = []
    for window in calendar_windows:
        if not window.get('available') or window['end'] <= start_time:
            continue
        avail_start = max(window['start'], start_time)
        avail_end = min(window['end'], planning_horizon_end)
        current = avail_start
        for usage in usage_periods:
            if usage['end_time'] <= current:
                continue
            if usage['start_time'] >= avail_end:
                break
            if usage['start_time'] > current:
                intervals.append((current, min(usage['start_time'], avail_end)))
            current = max(current, usage['end_time'])
            if current >= avail_end:
                break
        if current < avail_end:
            intervals.append((current, avail_end))
    return intervals

def find_earliest_resource_availability(
    operation_id: UUID,
    start_time: datetime.datetime,
    resource_schedules: Dict[str, Any],
    capability_mappings: Dict[str, Any],
    resource_usage: Dict[str, Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Find the earliest time when all required resources (machine and worker if required)
    are available for a given operation within their availability windows.
    
    This function checks:
    - Resource availability windows (shifts, working hours)
    - Current resource usage (already scheduled operations)
    
    It does NOT:
    - Actually schedule the operation
    - Modify availability windows
    
    Args:
        operation_id (UUID): ID of the operation to check
        start_time (datetime): Earliest possible start time to check from
        resource_schedules (Dict[str, Any]): Resource availability windows
        capability_mappings (Dict[str, Any]): Resource capability mappings
        resource_usage (Dict[str, Dict[str, Any]], optional): Current resource usage tracking.
            Format: {'machines': {machine_id: [{'start_time': datetime, 'end_time': datetime}, ...]}, 
                    'workers': {worker_id: [{'start_time': datetime, 'end_time': datetime}, ...]}}
        
    Returns:
        Dict[str, Any]: Dictionary containing:
            {
                'status': str,          # 'success' or 'error'
                'message': str,         # Description of result or error
                'start_time': datetime, # Earliest time when all resources are available
                'machine_id': UUID,     # ID of available machine (if required)
                'worker_id': UUID       # ID of available worker (if required)
            }
    """
    # Initialize empty resource usage if not provided
    if resource_usage is None:
        resource_usage = {'machines': {}, 'workers': {}}
    
    try:
        # Get operation requirements - using UUID object as key
        requirements = capability_mappings["operation_requirements"].get(operation_id)
        if not requirements:
            return {
                "status": "error",
                "message": f"Operation {operation_id} not found in capability mappings",
                "start_time": None,
                "machine_id": None,
                "worker_id": None
            }
        
        machine_required = requirements["machine_required"]
        worker_required = requirements.get("worker_required")  # null means no worker required
        operation_name = requirements.get("operation_name", "Unknown operation")
        
        # Get operation duration in hours for availability check and round to interval
        operation_duration = 0
        if "duration" in requirements:
            operation_duration = round_duration_to_interval(requirements["duration"] / 60)  # Convert minutes to hours and round
        if "setup_time" in requirements:
            operation_duration += round_duration_to_interval(requirements["setup_time"] / 60)  # Add setup time and round
        
        # Helper function to check if a resource is available for the entire duration
        def is_resource_available_for_duration(resource_type, resource_id, start, end):
            # If resource not tracked yet, it's available
            if resource_type not in resource_usage or resource_id not in resource_usage[resource_type]:
                return True
                
            # Check if there's any overlap with existing usage
            for usage in resource_usage[resource_type][resource_id]:
                # Check if there's any overlap between the proposed time slot and existing usage
                if (usage['start_time'] < end and usage['end_time'] > start):
                    return False
                    
            return True
        
        # Get available resources for this operation - using UUID object as key
        operation_resources = capability_mappings["operation_resources"].get(operation_id, {})
        
        # If no resources are required, we can schedule immediately
        if not machine_required and worker_required is None:
            return {
                "status": "success",
                "message": f"No resources required for operation {operation_name}",
                "start_time": round_to_interval(start_time),  # Round start time to interval
                "machine_id": None,
                "worker_id": None
            }
        
        # Get available machines if required
        available_machines = []
        if machine_required:
            available_machines = operation_resources.get("machines", [])
            if not available_machines:
                return {
                    "status": "error",
                    "message": f"No machines available for operation {operation_name} which requires a machine",
                    "start_time": None,
                    "machine_id": None,
                    "worker_id": None
                }
        
        # Get available workers if required
        available_workers = []
        if worker_required is not None:
            if worker_required == "SetupOnly":
                available_workers = operation_resources.get("setup_workers", [])
            elif worker_required == "OperationOnly":
                available_workers = operation_resources.get("operation_workers", [])
            elif worker_required == "SetupAndOperation":
                available_workers = operation_resources.get("setup_and_operation_workers", [])
            
            if not available_workers:
                return {
                    "status": "error",
                    "message": f"No workers with {worker_required} capability available for operation {operation_name}",
                    "start_time": None,
                    "machine_id": None,
                    "worker_id": None
                }
        
        # Find earliest time when all required resources are available
        earliest_start_time = None
        selected_machine_id = None
        selected_worker_id = None
        
        # Function to get availability periods for a resource
        def get_resource_periods(resource_type, resource_id):
            resource = resource_schedules[resource_type].get(resource_id)
            if not resource:
                return []
            return resource["availability"]
        
        # If only machine is required
        if machine_required and worker_required is None:
            for machine_id in available_machines:
                machine_periods = get_resource_periods("machines", machine_id)
                
                for period in machine_periods:
                    if period["available"] and period["end"] > start_time:
                        effective_start = round_to_interval(max(period["start"], start_time))  # Round start time to interval
                        
                        # Calculate end time of operation
                        operation_end = effective_start + timedelta(hours=operation_duration)
                        
                        # Check if operation fits within availability window
                        if operation_end <= period["end"]:
                            # Check if machine is available for the entire duration
                            if is_resource_available_for_duration("machines", machine_id, effective_start, operation_end):
                                if earliest_start_time is None or effective_start < earliest_start_time:
                                    earliest_start_time = effective_start
                                    selected_machine_id = machine_id
        
        # If only worker is required
        elif not machine_required and worker_required is not None:
            for worker_id in available_workers:
                worker_periods = get_resource_periods("workers", worker_id)
                
                for period in worker_periods:
                    if period["available"] and period["end"] > start_time:
                        effective_start = round_to_interval(max(period["start"], start_time))  # Round start time to interval
                        
                        # Calculate end time of operation
                        operation_end = effective_start + timedelta(hours=operation_duration)
                        
                        # Check if operation fits within availability window
                        if operation_end <= period["end"]:
                            # Check if worker is available for the entire duration
                            if is_resource_available_for_duration("workers", worker_id, effective_start, operation_end):
                                if earliest_start_time is None or effective_start < earliest_start_time:
                                    earliest_start_time = effective_start
                                    selected_worker_id = worker_id
        
        # If both machine and worker are required
        elif machine_required and worker_required is not None:
            # Collect all possible time slots
            possible_slots = []
            
            # Get all possible combinations of machine and worker periods
            for machine_id in available_machines:
                machine_periods = get_resource_periods("machines", machine_id)
                
                for worker_id in available_workers:
                    worker_periods = get_resource_periods("workers", worker_id)
                    
                    # Find overlapping availability periods
                    for m_period in machine_periods:
                        if not m_period["available"] or m_period["end"] <= start_time:
                            continue
                            
                        for w_period in worker_periods:
                            if not w_period["available"] or w_period["end"] <= start_time:
                                continue
                            
                            # Find overlap between periods
                            overlap_start = round_to_interval(max(m_period["start"], w_period["start"], start_time))  # Round start time to interval
                            overlap_end = min(m_period["end"], w_period["end"])
                            
                            # Calculate end time of operation
                            operation_end = overlap_start + timedelta(hours=operation_duration)
                            
                            # Check if operation fits entirely within overlap window
                            if operation_end <= overlap_end:
                                # Check if both resources are available for the entire duration
                                machine_available = is_resource_available_for_duration(
                                    "machines", machine_id, overlap_start, operation_end
                                )
                                worker_available = is_resource_available_for_duration(
                                    "workers", worker_id, overlap_start, operation_end
                                )
                                
                                if machine_available and worker_available:
                                    possible_slots.append({
                                        'start_time': overlap_start,
                                        'machine_id': machine_id,
                                        'worker_id': worker_id
                                    })
            
            # Find the earliest slot among all possibilities
            if possible_slots:
                earliest_slot = min(possible_slots, key=lambda x: x['start_time'])
                earliest_start_time = earliest_slot['start_time']
                selected_machine_id = earliest_slot['machine_id']
                selected_worker_id = earliest_slot['worker_id']
        
        if earliest_start_time is None:
            resource_type = []
            if machine_required:
                resource_type.append("machine")
            if worker_required is not None:
                resource_type.append("worker")
            return {
                "status": "error",
                "message": f"No {' and '.join(resource_type)} available at or after {start_time} for operation {operation_name}",
                "start_time": None,
                "machine_id": selected_machine_id,
                "worker_id": selected_worker_id
            }
        
        return {
            "status": "success",
            "message": f"Found available resources for operation {operation_name}",
            "start_time": earliest_start_time,
            "machine_id": selected_machine_id,
            "worker_id": selected_worker_id
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error finding resource availability: {str(e)}",
            "start_time": None,
            "machine_id": None,
            "worker_id": None
        }

def schedule_within_availability(operation_uuid, quantity, op, earliest_start_time, resource_schedules, resource_usage, scheduling_data, planning_horizon_end):
    """
    Schedule an operation respecting both resource availability windows and current resource usage.
    Also updates the passed resource_usage dictionary.
    """
    schedule_entries = []
    remaining_quantity = quantity
    current_start = round_to_interval(earliest_start_time)  # Round start time to nearest interval
    
    # Safeguard against zero or negative duration
    duration_minutes = max(1, op.get('duration', 0))  # Ensure minimum duration of 1 minute
    duration_hours = round_duration_to_interval(duration_minutes / 60)  # Round duration to nearest interval
    
    order_id = op.get('order_id')
    worker_required = scheduling_data['capability_mappings']['operation_requirements'][operation_uuid]['worker_required']
    
    # Safeguard against zero or negative quantity
    total_time_needed = duration_hours * max(1, quantity)  # Ensure minimum quantity of 1
    if current_start + timedelta(hours=total_time_needed) > planning_horizon_end:
        return []
        
    while remaining_quantity > 0:
        operation_resources = scheduling_data['capability_mappings']['operation_resources'].get(operation_uuid, {})
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
            machine_intervals = [(current_start, planning_horizon_end)] if not machine_id else get_resource_available_intervals('machines', machine_id, current_start, duration_hours, resource_usage, resource_schedules, planning_horizon_end)
            for worker_id in (worker_ids or [None]):
                worker_intervals = [(current_start, planning_horizon_end)] if not worker_id else get_resource_available_intervals('workers', worker_id, current_start, duration_hours, resource_usage, resource_schedules, planning_horizon_end)
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
                    setup_hours = round_duration_to_interval(max(1, setup_time_mins) / 60)  # Ensure minimum setup time of 1 minute
                    processing_start = round_to_interval(overlap_start + timedelta(hours=setup_hours))  # Round processing start to interval
                    
                    # Calculate available time, ensuring we don't divide by zero
                    time_diff = (overlap_end - processing_start).total_seconds() / 3600
                    if time_diff <= 0:
                        if m_end < w_end:
                            i += 1
                        else:
                            j += 1
                        continue
                        
                    available_time = time_diff
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
        # Update resource usage based on worker requirement type
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
                None,  # No worker required for operation
                best_resources['processing_start'],
                best_resources['processing_end'],
                'operation',
                order_id
            )
        elif worker_required == "OperationOnly":
            # Only require worker for operation period
            resource_usage = update_resource_usage(
                resource_usage,
                operation_uuid,
                best_resources['machine_id'],
                None,  # No worker required for setup
                best_slot,
                best_resources['processing_start'],
                'setup',
                order_id
            )
            resource_usage = update_resource_usage(
                resource_usage,
                operation_uuid,
                best_resources['machine_id'],
                best_resources['worker_id'],
                best_resources['processing_start'],
                best_resources['processing_end'],
                'operation',
                order_id
            )
        else:  # SetupAndOperation or None
            # Require worker for both setup and operation
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
            'total_duration_mins': (duration_minutes * best_resources['processable_quantity']) + best_resources['setup_time_mins'],
            'required_date': op.get('required_date')
        }
        schedule_entries.append(schedule_entry)
        remaining_quantity -= best_resources['processable_quantity']
        current_start = best_resources['processing_end']
    return schedule_entries 