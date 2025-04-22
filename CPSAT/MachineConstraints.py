from HelperFunctions.HelperFunctions import DTS

def add_force_proper_order_constraint(model_data, machine_task_intervals, machine_id, logger):
    #Kom dit achteraf nog eens nakijken en mss verplaatsen naar de interval preparation to lessen loops
    with logger.context("Force Proper Order"):
        logger.info("Adding constraint that forces tasks to occur in a proper order")
        chosen_list = []

        chosen_count = model_data.model.NewIntVar(0, len(machine_task_intervals), f"chosen_count_{machine_id}")
        logger.info(f"Created chosen_count interval and said it to be between 0 and {len(machine_task_intervals)}")
        for interval_data in machine_task_intervals:
            task_interval_id = interval_data['task_id']
            is_chosen = interval_data["is_chosen"]
            chosen_list.append(is_chosen)

            # Every task can have an order number between 0 and the number of tasks possible on the machine
            interval_data["order_var"] = model_data.model.NewIntVar(-1, len(machine_task_intervals) - 1, f"order_{task_interval_id}")

            # Every task that is not chosen gets order position -1
            model_data.model.Add(interval_data["order_var"] == -1).OnlyEnforceIf(is_chosen.Not())

            model_data.model.Add(interval_data["order_var"] < chosen_count).OnlyEnforceIf(is_chosen)
            model_data.model.Add(interval_data["order_var"] >= 0).OnlyEnforceIf(is_chosen)

        # Force that the chosen_count var has to be equal to the amount of tasks on the machine that are chosen
        model_data.model.Add(chosen_count == sum(chosen_list))

def add_machine_maintenance_constraint(model_data, machine_id, dataframes, logger):
    with logger.context("Machine Maintenance Constraint"):
        logger.info("Applying constraints that do not allow any tasks to run during machine maintenance")
        machines_unavailable_df = dataframes.machines_unavailable_df

        try:
            machine_windows_df = machines_unavailable_df[machines_unavailable_df['resourceId'].astype(str) == str(machine_id)]
            logger.AddDataFrameRecords("Found all planned maintenances for this machine", machine_windows_df)
            logger.info("Looping through maintenances")
            for index, window in machine_windows_df.iterrows():
                with logger.context(f"Window: {window}"):
                    start_s = DTS(window['startDate'], logger)
                    end_s = DTS(window['endDate'], logger)
                    duration = end_s - start_s
                    logger.info(f"Start: {start_s} s, end: {end_s}, duration: {duration}")

                    fixed_maint_interval = model_data.model.NewFixedSizeIntervalVar(
                        start=start_s,
                        size=duration,
                        name=f'maint_fixed_{machine_id}_{index}'
                    )
                    logger.info(f"Created interval: {fixed_maint_interval}")
                    model_data.machine_maintenance_window_intervals[machine_id].append(fixed_maint_interval)
                    logger.info("Added interval to model")
            logger.info(f"Added fixed maintenance intervals: {model_data.machine_maintenance_window_intervals[machine_id]}")
        except Exception as e:
            logger.error(f"Error processing maintenance windows for machine {machine_id}: {e}")

def add_no_overlap_constraint(model_data, machine_task_intervals, machine_id, logger):
    with logger.context("No Overlap Constraint"):
        logger.info("Creating constraint that makes sure no tasks, maintenance periods and preparation periods overlap")

        base_task_intervals_vars = []
        for data in machine_task_intervals:
            base_task_intervals_vars.append(data.get('interval'))
        logger.info("Retrieved Task Intervals")

        optional_prep_intervals = model_data.optional_prep_intervals_for_no_overlap.get(machine_id, [])
        logger.info("Retrieved Preparation Intervals")

        machine_maintenance_window_intervals = model_data.machine_maintenance_window_intervals.get(machine_id, [])
        logger.info("Retrieved Maintenance Intervals")

        all_intervals_on_machine = base_task_intervals_vars + optional_prep_intervals + machine_maintenance_window_intervals
        logger.info("Combined All Intervals")

        model_data.model.AddNoOverlap(all_intervals_on_machine).WithName(f'no_overlap_machine_{machine_id}')
        logger.info("Applied No Overlap Constraint")

def add_subserie_swap_window_constraints(model_data, machine_id, logger):
    logger.info(f"Adding start time constraints for subserie swaps on machine {machine_id} to allowed window.")

    if not hasattr(model_data, 'allowed_subserie_swap_domain'):
        logger.error(f"FATAL: model_data.allowed_subserie_swap_domain not found. Cannot apply swap window constraints.")
        return
    if model_data.allowed_subserie_swap_domain is None:
         logger.error(f"FATAL: model_data.allowed_subserie_swap_domain is None. Cannot apply swap window constraints.")
         return

    allowed_domain = model_data.allowed_subserie_swap_domain
    intervals_to_constrain = model_data.subserie_swap_intervals_machine

    if not intervals_to_constrain:
        logger.info(f"No subserie swap intervals found for machine {machine_id} in this pass.")
        model_data.subserie_swap_intervals_machine.clear()
        return

    logger.info(f"Found {len(intervals_to_constrain)} subserie swap intervals to constrain for machine {machine_id}.")

    constrained_count = 0
    for swap_interval in intervals_to_constrain:
        if hasattr(swap_interval, 'StartExpr'):
            start_var = swap_interval.StartExpr()
            allowed_values = allowed_domain.FlattenedIntervals() # Gets [lb1, ub1, lb2, ub2, ...]
            allowed_assignments_list = []
            if allowed_values:
                 for i in range(0, len(allowed_values), 2):
                     lb = allowed_values[i]
                     ub = allowed_values[i] # Corrected index to ub
                     for val in range(lb, ub + 1):
                         allowed_assignments_list.append((val,)) # Must be a tuple

            if allowed_assignments_list:
                 model_data.model.AddAllowedAssignments([start_var], allowed_assignments_list).WithName(f"swap_start_in_window_{machine_id}_{swap_interval.Name()}")
                 constrained_count += 1
            else:
                 logger.warning(f"Allowed domain resulted in empty assignment list for {swap_interval.Name()} on machine {machine_id}. No constraint added (task may become infeasible).")

        else:
            logger.warning(f"Item found in subserie_swap_intervals_machine for machine {machine_id} is not a valid interval object: {type(swap_interval)}")

    logger.info(f"Applied start time domain constraints to {constrained_count} swap intervals for machine {machine_id}.")

    model_data.subserie_swap_intervals_machine.clear()
