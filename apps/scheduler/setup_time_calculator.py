"""
Setup Time Calculator Module
===========================

This module handles the calculation of setup times for operations based on:
- Previous operation on the same machine
- Time gaps between operations
- Operation type matching

The module determines whether setup time should be applied based on the sequence
of operations on each machine and their timing.
"""

from datetime import datetime, timedelta
from typing import Dict, Optional, Any, List
from uuid import UUID

def calculate_setup_time(
    operation_id: UUID,
    machine_id: UUID,
    start_time: datetime,
    setup_time_mins: float,
    resource_usage: Dict[str, Dict[str, List[Dict[str, Any]]]],
    order_id: UUID = None  # Added order_id parameter
) -> float:
    """
    Calculate the actual setup time needed for an operation based on the previous
    operation on the same machine.

    Args:
        operation_id (UUID): ID of the operation being scheduled
        machine_id (UUID): ID of the machine being used
        start_time (datetime): Planned start time of the operation
        setup_time_mins (float): Standard setup time for the operation in minutes
        resource_usage (Dict): Current resource usage tracking
            Format: {
                'machines': {
                    machine_id: [{
                        'start_time': datetime,
                        'end_time': datetime,
                        'operation_id': UUID,
                        'order_id': UUID,  # Added to track order
                        'type': str  # 'setup' or 'operation'
                    }, ...]
                },
                'workers': {...}
            }
        order_id (UUID, optional): ID of the manufacturing order being scheduled

    Returns:
        float: Actual setup time needed in minutes. Will be 0 if no setup is required.
    """
    # If no setup time defined, return 0
    if not setup_time_mins or setup_time_mins <= 0:
        return 0

    # If no machine usage tracked yet, full setup time is needed
    if 'machines' not in resource_usage or machine_id not in resource_usage['machines']:
        return setup_time_mins

    # Get all operations on this machine
    machine_operations = resource_usage['machines'][machine_id]
    if not machine_operations:
        return setup_time_mins

    # Sort operations by end time to find the most recent one before our start time
    previous_operations = [
        op for op in machine_operations 
        if op['end_time'] <= start_time
    ]

    if not previous_operations:
        return setup_time_mins

    # Get the most recent operation (the one with the latest end time)
    previous_operation = max(previous_operations, key=lambda x: x['end_time'])

    # If the previous operation is the same operation and same order, no setup needed
    if (previous_operation['operation_id'] == operation_id and 
        previous_operation.get('order_id') == order_id):
        return 0

    return setup_time_mins

def update_resource_usage(
    resource_usage: Dict[str, Dict[str, List[Dict[str, Any]]]],
    operation_id: UUID,
    machine_id: Optional[UUID],
    worker_id: Optional[UUID],
    start_time: datetime,
    end_time: datetime,
    operation_type: str = 'operation',
    order_id: Optional[UUID] = None  # Added order_id parameter
) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """
    Update the resource usage tracking with a new operation.

    Args:
        resource_usage (Dict): Current resource usage tracking
        operation_id (UUID): ID of the operation
        machine_id (UUID): ID of the machine being used (if any)
        worker_id (UUID): ID of the worker being used (if any)
        start_time (datetime): Start time of the operation
        end_time (datetime): End time of the operation
        operation_type (str): Type of operation ('setup' or 'operation')
        order_id (UUID, optional): ID of the manufacturing order

    Returns:
        Dict: Updated resource usage tracking
    """
    # Initialize resource usage structure if needed
    if 'machines' not in resource_usage:
        resource_usage['machines'] = {}
    if 'workers' not in resource_usage:
        resource_usage['workers'] = {}

    # Add machine usage
    if machine_id:
        if machine_id not in resource_usage['machines']:
            resource_usage['machines'][machine_id] = []
        
        resource_usage['machines'][machine_id].append({
            'start_time': start_time,
            'end_time': end_time,
            'operation_id': operation_id,
            'order_id': order_id,  # Added order_id
            'type': operation_type
        })
        
        # Sort machine operations by start time
        resource_usage['machines'][machine_id].sort(key=lambda x: x['start_time'])

    # Add worker usage
    if worker_id:
        if worker_id not in resource_usage['workers']:
            resource_usage['workers'][worker_id] = []
        
        resource_usage['workers'][worker_id].append({
            'start_time': start_time,
            'end_time': end_time,
            'operation_id': operation_id,
            'order_id': order_id,  # Added order_id
            'type': operation_type
        })
        
        # Sort worker operations by start time
        resource_usage['workers'][worker_id].sort(key=lambda x: x['start_time'])

    return resource_usage 