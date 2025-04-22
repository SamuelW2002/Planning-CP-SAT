from datetime import datetime, time, timedelta
import math
import pandas as pd
from sortedcontainers import SortedDict
from ortools.sat.python import cp_model
from HelperFunctions.HelperFunctions import DTS

# The furthest an order can be planned is half a year after the time it was due originally
MAX_DATE = 15778800

def create_intervals_for_orders(model_data, possible_machines_for_orders_df, logger):
    with logger.context("Creating Intervals"):
        logger.info("Started creating Intervals for every single task")
        for order_id in model_data.unique_order_ids:
            with logger.context(f"Interval for order: {order_id}"):
                all_possible_tasks_for_order = possible_machines_for_orders_df[possible_machines_for_orders_df['orderID'] == order_id]
                logger.info("Found all possible tasks")

                logger.info("Iterating through every single task")
                machine_choices = []
                for index, task in all_possible_tasks_for_order.iterrows():
                    with logger.context(f"Task {index}"):
                        interval_data = create_task_details(order_id, task, logger)

                        start_var = model_data.model.NewIntVar(0, MAX_DATE, f'start_{interval_data["task_id"]}')
                        duration_var = model_data.model.NewConstant(interval_data['duration'])
                        end_var = model_data.model.NewIntVar(0, MAX_DATE, f'end_{interval_data["task_id"]}')
                        is_chosen = model_data.model.NewBoolVar(f'machine_choice_{interval_data["task_id"]}')
                        logger.info("Created start, end, duration and is_chosen variable for task")

                        interval_var = model_data.model.NewOptionalIntervalVar(start_var, duration_var, end_var, is_chosen, f'{interval_data["task_id"]}')
                        machine_choices.append(is_chosen)
                        logger.info("Added is_chosen bool to boolean list for order to apply AddExactlyOne")

                        interval_data["interval"] = interval_var
                        interval_data["is_chosen"] = is_chosen
                        logger.info("Added is chosen variable and interval to interval data")

                        model_data.machine_intervals[interval_data['machine_id']].append(interval_data)
                        logger.info(f"Added interval to machine_intervals dict on machine_id")

                model_data.model.AddExactlyOne(machine_choices)
                logger.info(f"Applied AddExactlyOne constraint on all possible tasks for for order id: {order_id}")

def create_task_details(order_id, task, logger):
    logger.info("Extracting task details")
    subserie_id = task['subserieID']
    machine_id = task['machineID']
    iml_possible = task['IML possible']
    duration = int(task['secondsNeeded'])
    dueDate = task['dueDate']
    info = task['info'] if pd.notna(task['info']) else ''
    priority_code = task['priority_code']
    machine_status = task['machine_status']
    description = task["description"]
    hotrunner = task["hotrunner"]
    matrijsName = task["matrijsName"]

    task_id = f"{order_id}_{subserie_id}_{machine_id}_{iml_possible}"
    logger.info(f"Unique task id is: {task_id}")

    is_past_due_date = False
    impossible_deadline = False

    if dueDate <= datetime.now():
        is_past_due_date = True
        logger.info("Deadline of Task is in the past")
    elif dueDate <= datetime.now() + timedelta(seconds=duration):
        impossible_deadline = True
        logger.info("Deadline for task cannot be reached")

    task_details = {
        'mongo_id': str(task['mongoID']),
        'task_id': task_id,
        'order_id': order_id,
        'subserie_id': subserie_id,
        'machine_id': machine_id,
        'duration': duration,
        'due_date': dueDate,
        'is_past_due_date': is_past_due_date,
        'impossible_deadline': impossible_deadline,
        'priority_code': int(priority_code),
        'info': info,
        'iml_possible': iml_possible,
        'machine_status': int(machine_status),
        'description': description,
        'hotrunner': hotrunner,
        'matrijsName': matrijsName
    }
    logger.info(f"Task Details: {task_details}")
    return task_details

def create_capacity_reduction_intervals(model_data, ombouwers_beschikbaar_df, default_capacity, logger):
    with logger.context("Capacity Reduction Intervals"):
        logger.context("Creating intervals that indicate how many subserie swaps can occur at once")
        now = datetime.now()

        logger.info(f"Set base capacity to {default_capacity}")
        capacity_map = SortedDict()
        reduction_dates_from_df = {}
        # Create a map containing the days where the value changes and then a marker on the next day indicating that it has to go back to default value
        if ombouwers_beschikbaar_df is not None and not ombouwers_beschikbaar_df.empty:
            logger.info("Technicians available DatFrame was not empty")
            try:
                df_copy = ombouwers_beschikbaar_df.copy()
                df_copy['date'] = pd.to_datetime(df_copy['date']).dt.date

                for index, row in df_copy.iterrows():
                    day_date = row['date']
                    day_capacity = int(row['ombouwersBeschikbaar'])

                    reduction_dates_from_df[day_date] = day_capacity
                logger.info(f"Created a dict that contains all the dates and values: {reduction_dates_from_df}")

                with logger.context("Looping Through Dates"):
                    for day_date in sorted(reduction_dates_from_df.keys()):
                        capacity_map[day_date] = reduction_dates_from_df[day_date]
                        logger.info(f"Added date: {day_date} to capacity map")
                        next_day = day_date + timedelta(days=1)
                        if next_day not in reduction_dates_from_df:
                            logger.info("Next day was not a reduced capacity day aswell")
                            if next_day not in capacity_map or capacity_map[next_day] != default_capacity:
                                capacity_map[next_day] = default_capacity
                                logger.info("Added next day as a default capacity marker")
                logger.info("Finished creating capacity map")
            except Exception as e:
                logger.error(f"Error occured: {e}")

        horizon_marker_date = (now + timedelta(seconds=MAX_DATE)).date()
        logger.info(f"Set horizon for intervals to {horizon_marker_date}")
    
        change_point_dates = list(capacity_map.keys())
        logger.info(f"Retrieved all dates where a change occurs, including reverting to normal capacity: {change_point_dates}")

        current_processing_date = now.date()
        date_iterator = iter(change_point_dates)
        next_change_date = next(date_iterator, None)
        with logger.context("Looping Through Dates"):
            i = 0
            while current_processing_date < horizon_marker_date:
                with logger.context(f"Date: {current_processing_date}"):
                    i = i + 1
                    effective_capacity = default_capacity
                    logger.info(f"Set effective capacity equal to {effective_capacity}")

                    relevant_change_dates = [day for day in capacity_map.keys() if day <= current_processing_date]
                    logger.info(f"Found all dates where changes occur before current processing date: {relevant_change_dates}")
                    if relevant_change_dates:
                        last_relevant_change_date = relevant_change_dates[-1]
                        effective_capacity = capacity_map.get(last_relevant_change_date)
                        logger.info(f"Found most recent change date at: {last_relevant_change_date} with capacity: {effective_capacity}. Setting values...")
                    block_end_date = horizon_marker_date
                    logger.info("Set the block end date equal to the horizon")

                    if next_change_date is not None:
                        block_end_date = min(next_change_date, horizon_marker_date)
                        logger.info(f"Found another date after this one, setting block end date to: {block_end_date}")
                    if effective_capacity < default_capacity:
                        # Calculate how many capacity units the blocker needs to consume
                        demand_reduction = default_capacity - effective_capacity
                        logger.info(f"Calculated default capacity - effective capacity to find the demand, demand: {demand_reduction}")
                        block_start_seconds = int(((datetime.combine(current_processing_date, time.min)) - now).total_seconds())
                        logger.info(f"Calculated start for this block to be {block_start_seconds} seconds from now")
                        block_end_seconds = int(((datetime.combine(block_end_date, time.max)) - now).total_seconds())
                        logger.info(f"Calculated end for this block to be {block_end_seconds} seconds from now")
                        actual_duration = block_end_seconds - block_start_seconds
                        logger.info(f"Calculated block duration to be: {actual_duration} seconds")

                        reduction_interval = model_data.model.NewFixedSizeIntervalVar(
                            start=block_start_seconds,
                            size=actual_duration,
                            name=f'cap_reduc_{current_processing_date.strftime("%Y%m%d")}_to_{block_end_date.strftime("%Y%m%d")}'
                        )
                        logger.info(f"Created the fixed size interval: {reduction_interval}")

                        model_data.cp_capacity_reduction_intervals.append(reduction_interval)
                        logger.info("Added the interval to the list in the model data")
                        model_data.cp_capacity_reduction_demands.append(demand_reduction)
                        logger.info("Added the demand to the list in the model data")
                    current_processing_date = block_end_date
                    if next_change_date is not None and current_processing_date >= next_change_date:
                        next_change_date = next(date_iterator, None)
        logger.info("Finished creating intervals")

def calculate_allowed_prep_range_sub_swap(model_data, dataframes, logger):
    with logger.context("Subserie Swap Time Ranges"):
        logger.info("Creating time intervals for when swapping subseries is allowed")
        now = datetime.now()

        available_weekends_df = dataframes.available_weekends_df
        available_weekend_dates_set = set()

        if not available_weekends_df.empty:
            available_dates = pd.to_datetime(dataframes.available_weekends_df['date']).dt.date
            available_weekend_dates_set = set(available_dates)

        horizon_days = math.ceil(MAX_DATE / (24 * 3600)) + 1
        logger.info(f"Set last day in range creation to {horizon_days} days from now")
        
        logger.info("Looping through all of the dates and checking if they are a week day or an available weekend")
        allowed_intervals_list = []
        for day_offset in range(horizon_days):
            target_date = (now + timedelta(days=day_offset)).date()
            is_weekday = target_date.weekday() < 5
            is_available_weekend = target_date in available_weekend_dates_set

            if is_weekday or is_available_weekend:
                # Define 6:00 AM and 1:00 PM in seconds from now
                start_seconds = DTS(datetime.combine(target_date, time(6, 0, 0)), logger)
                end_seconds = DTS(datetime.combine(target_date, time(13, 0, 0)), logger)
                
                allowed_intervals_list.append((start_seconds, end_seconds))
        logger.info("Added intervals that define what time a subserie swap is allowed")
        logger.info(f"Intervals: {allowed_intervals_list}")

        model_data.allowed_subserie_swap_domain = cp_model.Domain.FromIntervals(allowed_intervals_list)
        logger.info(f"Created allowed start domain for subserie swaps: {model_data.allowed_subserie_swap_domain}")



def create_cumulative_blocking_intervals(model_data, logger):
    with logger.context("Cumulative Blocking Intervals"):
        logger.info("Creating mandatory intervals for forbidden swap times (11 PM - 1 AM)")

        now = datetime.now()
        model = model_data.model

        min_model_time = 0

        horizon_duration_days = math.ceil(MAX_DATE / (24 * 3600))

        logger.info(f"Checking approx {horizon_duration_days} days for blocking interval generation.")

        blocking_intervals = []

        for day_offset in range(-1, horizon_duration_days):
            current_day_date = (now + timedelta(days=day_offset)).date()
            next_day_date = (now + timedelta(days=day_offset + 1)).date()

            blocker_start_dt = datetime.combine(current_day_date, time(13, 0, 0))
            blocker_end_dt = datetime.combine(next_day_date, time(6, 0, 0))

            blocker_start_seconds_raw = DTS(blocker_start_dt, logger)
            blocker_end_seconds_raw = DTS(blocker_end_dt, logger)

            # --- Clamp the interval to the model's time boundaries and check validity ---

            # Ignore intervals ending before the model starts
            if blocker_end_seconds_raw <= min_model_time:
                continue
            # Stop creating intervals if they start after the model ends



            # Calculate effective duration, skip if interval is outside or has no duration within bounds
            effective_duration = blocker_end_seconds_raw - blocker_start_seconds_raw

            # --- Create the mandatory interval for the CP-SAT model ---
            # CP-SAT intervals require integer start and size. Round carefully.
            start_int = int(round(blocker_start_seconds_raw))
            duration_int = int(round(effective_duration))

            # Final check for non-positive duration after rounding
            if duration_int <= 0:
                 continue

            # Create the mandatory interval variable (no 'is_present' argument)
            blocker_interval = model.NewFixedSizeIntervalVar(
                start=start_int,
                size=duration_int,
                name=f'blocker_{current_day_date.strftime("%Y%m%d")}'
            )
            blocking_intervals.append(blocker_interval)


        # Store the list of blocking interval variables in model_data
        model_data.blocking_intervals_for_cumulative = blocking_intervals
        logger.info(f"Created {len(blocking_intervals)} mandatory blocking intervals for cumulative constraints.")
        # Log a few example intervals for verification if needed
        if blocking_intervals:
            # Displaying the interval object itself might not be super informative,
            # maybe log the name or calculated start/duration again if needed for debug
            logger.info(f"Example blocking intervals (names): {[bi.Name() for bi in blocking_intervals[:min(3, len(blocking_intervals))]]}")