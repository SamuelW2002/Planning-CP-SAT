from dataclasses import dataclass, field
from typing import Dict, List, Any
from ortools.sat.python import cp_model

@dataclass
class SchedulingModelData:
    model: cp_model.CpModel = cp_model.CpModel()
    solver: cp_model.CpSolver = cp_model.CpSolver()
    max_ceiling_variable: int = 15778800 # Halve year
    machine_to_intervals_map: Dict[Any, List[Dict]] = field(default_factory=dict)

    # Holds all of the unique order ID's
    unique_order_ids: List[Any] = field(default_factory=list)

    # Holds all of the penalties that the model will try to minimize
    all_penalties: List[Any] = field(default_factory=list)
    
    # Holds all of the penalty intervals, chosen or not, later extracted in processing to convert in returned json objects
    setup_penalty_intervals: List[Any] = field(default_factory=list)
    
    # Holds all of the end times (in seconds from now) from orders that were chosen (otherwise 0 when not chosen)
    actual_end_vars: List[Any] = field(default_factory=list)

    # Holds the ID's of machines that were claimed by an emergency order
    emergency_used_machine_ids: List[Any] = field(default_factory=list)

    # Holds all of the tasks that were running at that very moment
    currently_running_tasks: List[Any] = field(default_factory=list)

    # Holds all of the intervals that will later be used to use in the NoOverlap
    optional_prep_intervals_for_no_overlap: Dict[Any, Any] = field(default_factory=dict)

    # Holds all of the intervals representing maintenance windows for a machine, used in NoOverlap  Constraint
    machine_maintenance_window_intervals: Dict[Any, Any] = field(default_factory=dict)

    # Holds all preparation intervals for subserie swaps so that we can later limit the amount of intervals occuring at the same time
    cp_subserie_swap_interval_vars: List[Any] = field(default_factory=list)

    # Holds all preparation intervals for subserie swaps for each machine individually to make sure subserie swaps happen in certain windows
    subserie_swap_intervals_machine: List[Any] = field(default_factory=list)

    # Holds all preparation intervals for IML swaps so that we can later use this to make sure IML swaps do not occur on the weekend
    cp_IML_swap_intervals: List[Any] = field(default_factory=list)

    # Holds all of the intervals associated with a reduction: [Interval, Interval, Interval, ...]
    cp_capacity_reduction_intervals: List[Any] = field(default_factory=list)
    
    # Holds all of the actual reduction values for the Interval: [0, 0, 0, 0, 1, 1, 1, 0, ...]
    cp_capacity_reduction_demands: List[Any] = field(default_factory=list)

    # Holds all of the Intervals that define when a subserie swap is allowed to happen, between 6 AM and 2 PM
    allowed_subserie_swap_domain: List[Any] = field(default_factory=list)

