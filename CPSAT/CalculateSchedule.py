from Domain.ScheduleDataClass import SchedulingModelData
from CPSAT.TaskConstraints import add_task_vs_task_penalties, add_deadline_slack_constraint, add_makespan_constraint, add_weekend_constraints_tasks, add_subserie_unavailable_constraint
from CPSAT.ProcesResultFunctions import extract_machine_schedules
from CPSAT.MachineConstraints import add_force_proper_order_constraint, add_machine_maintenance_constraint, add_no_overlap_constraint, add_subserie_swap_window_constraints
from CPSAT.PrepareIntervalsFunctions import create_intervals_for_orders, create_capacity_reduction_intervals, calculate_allowed_prep_range_sub_swap, create_cumulative_blocking_intervals
from CPSAT.GeneralConstraintFunctions import add_general_constraints

DEFAULT_SUB_SWAP_AMOUNT_CAP = 3

def create_optimized_schedule_df(dataframes, logger, duration):
    with logger.context("Creating Optimal Schedule"):
        logger.info("Starting the creation of the model")

        model_data = SchedulingModelData()
        model = model_data.model
        solver = model_data.solver
        logger.info("Initialised model data instance")

        machine_names_df = dataframes.machine_names_df
        possible_machines_for_orders_df = dataframes.all_tasks_df
        ombouwers_beschikbaar_df = dataframes.technician_unavailability_df

        machine_ids = machine_names_df['_k2_machineID'].unique()
        model_data.unique_order_ids = possible_machines_for_orders_df['orderID'].unique()
        logger.info("Found all unique order and machine ID's")

        model_data.machine_intervals = {machine_id: [] for machine_id in machine_ids}
        model_data.optional_prep_intervals_for_no_overlap = {machine_id: [] for machine_id in machine_ids}
        model_data.machine_maintenance_window_intervals = {machine_id: [] for machine_id in machine_ids}
        logger.info("Initialised empty dict in model data for machine_intervals, optional_prep_intervals and machine maintenance intervals")

        create_intervals_for_orders(model_data, possible_machines_for_orders_df, logger)
        create_capacity_reduction_intervals(model_data, ombouwers_beschikbaar_df, DEFAULT_SUB_SWAP_AMOUNT_CAP, logger)
        calculate_allowed_prep_range_sub_swap(model_data, dataframes, logger)
        create_cumulative_blocking_intervals(model_data, logger)

        add_all_constraints(model_data, dataframes, logger)

        model.Minimize(sum(model_data.all_penalties)) 

        logger.info(f"Setting max solver time to {duration} seconds")
        solver.parameters.max_time_in_seconds = duration
        solver.parameters.log_search_progress = True
        logger.info(f"Setting max solver workers to 4")

        #For deploy only
        solver.parameters.num_search_workers = 4
        #solver.parameters.stop_after_first_solution = True
        
        # When finding infeasible solution the solver will learn from this to avoid exploring the same area again later
        #solver.parameters.optimize_with_core = True

        # Aggresivly searches for infeasabilities in the beginning to eliminate large portions of search space
        #solver.parameters.find_multiple_cores = True

        # No Linear Programming, solver will onyl use CP SAT, bad for optimal, might be better for feasible
        #solver.parameters.use_lp = False

        status = solver.Solve(model)
        logger.feedback(f"Solver took {solver.WallTime()} seconds to find this solution")

        scheduled_orders_df, preparation_intervals_df = extract_machine_schedules(
            model_data, solver, status, logger
        )   

        dataframes.scheduled_orders_df = scheduled_orders_df
        dataframes.preparation_intervals_df = preparation_intervals_df  #

def add_all_constraints(model_data, dataframes, logger):
    with logger.context("Adding All Constraints"):
        logger.info("Looping through all machines")
        for machine_id, machine_task_intervals in model_data.machine_intervals.items():
            with logger.context(f"Machine: {machine_id}"):
                add_force_proper_order_constraint(model_data, machine_task_intervals, machine_id, logger)
                add_machine_maintenance_constraint(model_data, machine_id, dataframes, logger)
                logger.info("Looping through all of the task intervals")
                for interval_data in machine_task_intervals:
                    with logger.context(f'Task: {interval_data["task_id"]}'):
                        add_deadline_slack_constraint(model_data, interval_data, logger)
                        add_task_vs_task_penalties(model_data, interval_data, machine_task_intervals, machine_id, logger)
                        add_subserie_unavailable_constraint(model_data, interval_data, dataframes, logger)
                        add_makespan_constraint(model_data, interval_data, logger)
                        add_weekend_constraints_tasks(model_data, interval_data, dataframes, logger)

                # # Above functions add intervals to be included in these functions so they need to be called after internal loop
                add_no_overlap_constraint(model_data, machine_task_intervals, machine_id, logger)
                add_subserie_swap_window_constraints(model_data, machine_id, logger)
        add_general_constraints(model_data, DEFAULT_SUB_SWAP_AMOUNT_CAP, dataframes, logger)

        logger.context("Finished adding all of the constraints")


