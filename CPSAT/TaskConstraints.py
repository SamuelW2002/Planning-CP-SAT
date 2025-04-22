from datetime import datetime, time
from HelperFunctions.HelperFunctions import DTS
 
def add_task_vs_task_penalties(model_data, interval_data_1, machine_task_intervals, machine_id, logger):
    logger.info("Adding constraints that are based on the interaction between 2 tasks")
    if len(machine_task_intervals) < 2:
        logger.info("There are less than 2 tasks possible for this machine, skipping steps...")
        return

    for interval_data_2 in machine_task_intervals:
        if interval_data_1 == interval_data_2:
            continue

        order_var_1 = interval_data_1["order_var"]
        order_var_2 = interval_data_2["order_var"]
        is_chosen_1 = interval_data_1["is_chosen"]
        is_chosen_2 = interval_data_2["is_chosen"]
        task_1_id = interval_data_1["task_id"]
        task_2_id = interval_data_2["task_id"]

        model_data.model.Add(order_var_1 != order_var_2).OnlyEnforceIf(is_chosen_1, is_chosen_2)

        order_follow_var = model_data.model.NewBoolVar(f"order_follow_{task_1_id}_{task_2_id}")
        model_data.model.Add(order_var_2 == order_var_1 + 1).OnlyEnforceIf(order_follow_var)
        model_data.model.Add(order_var_2 != order_var_1 + 1).OnlyEnforceIf(order_follow_var.Not())
        # Created constraints that say order folow boolean is true when task 2 order is task 1 order + 1

        follow_and_chosen = model_data.model.NewBoolVar(f"follow_and_chosen_{task_1_id}_{task_2_id}")
        model_data.model.AddBoolAnd([order_follow_var, is_chosen_1, is_chosen_2]).OnlyEnforceIf(follow_and_chosen)
        model_data.model.AddBoolOr([order_follow_var.Not(), is_chosen_1.Not(), is_chosen_2.Not()]).OnlyEnforceIf(follow_and_chosen.Not())
        # Enforced constraint saying that follow and chosen boolean is true if both tasks are chosen and task 2 follows task 1

        if interval_data_1["priority_code"] == 7 and not interval_data_2["priority_code"] == 7:
            model_data.all_penalties.append(follow_and_chosen * 3000)

        add_preparation_time_penalty(model_data, follow_and_chosen, machine_id, interval_data_1, interval_data_2)


def add_preparation_time_penalty(model_data, follow_and_chosen, machine_id, interval_data_1, interval_data_2):
    task_1_id = interval_data_1["task_id"]
    task_2_id = interval_data_2["task_id"]
    setup_penalty_seconds, setup_penalty_type = calculate_setup_penalty(interval_data_1, interval_data_2)

    if setup_penalty_seconds == 0:
        return
    

    prep_start_var = model_data.model.NewIntVar(0, model_data.max_ceiling_variable, f'prep_start_{task_1_id}_to_{task_2_id}')

    prep_interval_var = model_data.model.NewOptionalFixedSizeIntervalVar(
        start=prep_start_var,
        size=setup_penalty_seconds,
        is_present=follow_and_chosen,
        name=f'prep_iv_subserie_{task_1_id}_to_{task_2_id}'
    )
    # Created optional interval that is present if follow and chosen between task 1 and 2 is true with a duration equal to the penalty time

    model_data.model.Add(prep_interval_var.StartExpr() >= interval_data_1["interval"].EndExpr()).OnlyEnforceIf(follow_and_chosen).WithName(f"prep_after_t1_{task_1_id}_to_{task_2_id}")
    # Enforced constraint saying that the preparation can only start after task 1 ends

    model_data.model.Add(interval_data_2["interval"].StartExpr() >= prep_interval_var.EndExpr()).OnlyEnforceIf(follow_and_chosen).WithName(f"t2_after_prep_{task_1_id}_to_{task_2_id}")
    # Enforced constraint saying task 2 can only start after preparation ends

    model_data.setup_penalty_intervals.append({
        'enforce_bool_var': follow_and_chosen,
        'penalty_start_var': prep_interval_var.StartExpr(),
        'penalty_duration': setup_penalty_seconds,
        'penalty_type': setup_penalty_type,
        'machine_id': machine_id,
        'task_1_description': interval_data_1["description"],
        'task_2_description': interval_data_2["description"],
        'task_1_matrijs': interval_data_1["matrijsName"],
        'task_2_matrijs': interval_data_2["matrijsName"],
        'task_1_hotrunner': interval_data_1["hotrunner"],
        'task_2_hotrunner': interval_data_2["hotrunner"],
        'mongo_id': interval_data_2['mongo_id']
    })

    if setup_penalty_type == "ombouw":
        model_data.cp_subserie_swap_interval_vars.append(prep_interval_var)
        model_data.subserie_swap_intervals_machine.append(prep_interval_var)
    elif setup_penalty_type ==  "ombouw2":
        prep_machine_dict = {"machine_id" : machine_id, "prep_interval" : prep_interval_var}
        model_data.cp_IML_swap_intervals.append(prep_machine_dict)
    
    model_data.optional_prep_intervals_for_no_overlap[machine_id].append(prep_interval_var)

def calculate_setup_penalty(task1, task2):
    if task1['subserie_id'] != task2['subserie_id']:
        return 4 * 3600, "ombouw"
    elif task1['iml_possible'] or task2['iml_possible']:
        return 1 * 3600, "ombouw2"
    return 0, ""


def add_deadline_slack_constraint(model_data, interval_data, logger, MAX_HORIZON_DATE):
    with logger.context("Deadline Slack Constraint"):
        logger.info("Adding constraint that calculates how much a task missed it's deadline")
        due_date = interval_data['due_date']
        task_id = interval_data['task_id']
        due_date_seconds = DTS(due_date, logger)
        logger.info(f"Due date in seconds from now (past or future): {due_date_seconds}")

        interval = interval_data["interval"]
        deadline_slack_var = model_data.model.NewIntVar(0, due_date_seconds + MAX_HORIZON_DATE * 10, f'deadline_slack_{task_id}')

        model_data.model.Add(deadline_slack_var >= interval.EndExpr() - due_date_seconds).OnlyEnforceIf(interval_data['is_chosen']).WithName(f'interval_for_order_force_deadline_slack_larger_than_deadline_constraint_{task_id}')
        logger.info("Forced deadline slack to be larger or equal to the end time of the task minus the due date in seconds, 0 or time past deadline")

        model_data.model.Add(deadline_slack_var == 0).OnlyEnforceIf(interval_data['is_chosen'].Not()).WithName(f'interval_for_order_force_deadline_slack_equal_to_0_{task_id}')
        logger.info("Forced deadline slack to be 0 if task is not chosen")

        interval_data['deadline_slack_var'] = deadline_slack_var

def add_makespan_constraint(model_data, interval_data, logger):
    with logger.context("Makespan constraint"):
        logger.info("Creating the constraint that decided the priority of the task")
        task_id = interval_data['task_id']
        interval = interval_data["interval"]
        priority_code = interval_data['priority_code']
        machine_id = interval_data['machine_id']
        deadline_slack = interval_data['deadline_slack_var']
        is_past_due_date = interval_data["is_past_due_date"]
        is_chosen = interval_data["is_chosen"]

        chosen_end_var = model_data.model.NewIntVar(
            0,
            model_data.max_ceiling_variable,
            f'chosen_end_var_{task_id}'
        )

        model_data.model.Add(chosen_end_var == interval.EndExpr()).OnlyEnforceIf(is_chosen)
        # Added constraint saying if the task is chosen then the chosen end var is equal to the end var

        model_data.model.Add(chosen_end_var == 0).OnlyEnforceIf(is_chosen.Not())
        logger.info("Added constraint saying if the task is not chosen the chosen end var is 0")

        model_data.all_penalties.append(chosen_end_var)
        logger.info("Added the chosen end var to the list of all end vars for final minimize")

        if priority_code == 1: 
            logger.info("Task is a priority order")
            model_data.model.Add(interval.StartExpr() == 0).WithName(f'makespan_constraint_emergency_order_force_start_var_below_1_{task_id}')
            logger.info("Added constraint saying the start of the interval has to be 0")

            model_data.all_penalties.append(interval.EndExpr() * 10000)
            logger.info("Added penalty of end time multiplied by 10000 to make sure most optimal machine is chosen for order")

            model_data.emergency_used_machine_ids.append(machine_id)

        elif priority_code == 2:
            logger.info("Task has a priority level of 2")
            model_data.currently_running_tasks.append(interval_data)

        elif priority_code == 3:
            logger.info("Task has priority code 3")
            model_data.all_penalties.append(chosen_end_var * 50)

        elif priority_code == 4:
            logger.info("Task has priority code 4")
            model_data.all_penalties.append(deadline_slack * 20)
            
        elif priority_code == 5:
            logger.info("Task has priority code 5")
            if is_past_due_date:
                logger.info("Deadline of the task is in the past")
                model_data.all_penalties.append(deadline_slack * 10)
                logger.info("Penalty is deadline slack * 10")
            else:
                model_data.all_penalties.append(deadline_slack * 5)
                logger.info("Penalty is deadline slack * 5")

        logger.info("Looping through every single task that is currently running on machines")
        for interval_data in model_data.currently_running_tasks:
            task_id = interval_data["task_id"]
            machine_id = interval_data['machine_id']
            interval = interval_data["interval"]
            logger.info(f"Task currently running on machine {machine_id} is {task_id}")

            if machine_id not in model_data.emergency_used_machine_ids:
                logger.info("Machine is not being used by an emergency order")
                model_data.model.Add(interval.StartExpr() == 0).WithName(f'makespan_constraint_emergency_order_force_start_var_below_1_{task_id}')
                logger.info("Forced start of order to be 0")
            else:
                logger.info("This machine is being occupied by an emergency order")
                emergency_and_running_end_time_penalty_sum += interval.EndExpr() * 1000
                logger.info("This order has to be continued as soon as emergency ends, penalty = end of order * 1000")

def add_subserie_unavailable_constraint(model_data, interval_data, dataframes, logger):
    with logger.context("Subserie Unavailable Constraint"):
        logger.info("Applying constraints where certain subseries cannot be produced for certain time periods")
        subseries_unavailable_df = dataframes.subseries_unavailable_df
        if subseries_unavailable_df.empty:
            logger.info("No subserie unavailabilities were found")
            return

        subserie_id = interval_data['subserie_id']
        interval = interval_data['interval']
        task_id = interval_data['task_id']
        model = model_data.model
        logger.info(f"Current task is for subserie: {subserie_id}")

        relevant_windows_df = subseries_unavailable_df[subseries_unavailable_df['subserieId'] == subserie_id]
        logger.addDataFrameRecords("Time ranges for this subserie", relevant_windows_df)

        if relevant_windows_df.empty:
            logger.info("No subserie unavailability ranges given for this subserie")
            return

        logger.info("Looping through all of the unavailability ranges")
        for index, window in relevant_windows_df.iterrows():
            start_dt = window['startDate']
            end_dt = window['endDate']

            block_start = DTS(start_dt, logger)
            block_end = DTS(end_dt, logger)

            ends_before_block = model.NewBoolVar(f'ebb_{task_id}_win{index}')

            model.Add(interval.EndExpr() <= block_start).OnlyEnforceIf(ends_before_block)
            model.Add(interval.EndExpr() > block_start).OnlyEnforceIf(ends_before_block.Not())

            starts_after_block = model.NewBoolVar(f'sab_{task_id}_win{index}')

            model.Add(interval.StartExpr() >= block_end).OnlyEnforceIf(starts_after_block)
            model.Add(interval.StartExpr() < block_end).OnlyEnforceIf(starts_after_block.Not())
            # Added constraint saying start after block bool is true if task starts after block ends

            no_overlap_condition = model.NewBoolVar(f'no_ov_{task_id}_win{index}')
            model.AddBoolOr([ends_before_block, starts_after_block]).OnlyEnforceIf(no_overlap_condition)
            model.AddBoolAnd([ends_before_block.Not(), starts_after_block.Not()]).OnlyEnforceIf(no_overlap_condition.Not())
            # Added constraint saying no overlap occurs if the task starts after the block or ends before the block

            model.AddImplication(interval_data["is_chosen"], no_overlap_condition).WithName(f'subserie_no_overlap_{task_id}_win{index}')
            # Added constraint saying if the task is chosen then no overlap can occur

def add_weekend_constraints_tasks(model_data, interval_data, dataframes, logger):
    with logger.context("Weekend Constraint"):
        logger.info("Started creating constraints saying no work can be STARTED or ENDED in the weekend unless specified otherwise")
        weekends_unavailable_df = dataframes.weekends_unavailable_df
        if weekends_unavailable_df.empty:
            logger.info("No unavailable weekends were found")
            return
        
        machine_id = interval_data['machine_id']
        task_id = interval_data['task_id']
        duration = interval_data['duration']

        start_var = interval_data['interval'].StartExpr()
        end_var = interval_data['interval'].EndExpr()

        machine_weekends_df = weekends_unavailable_df[
            weekends_unavailable_df['machineId'] == machine_id
        ]
        logger.addDataFrameRecords("All unavailable weekend dates for this machine", machine_weekends_df)

        weekend_flags = []
        weekend_penalty_vars = []
        interval_data.setdefault('weekend_flag_map', {})

        penalty_per_weekend_day = 24 * 3600

        logger.info("Looping through dates")
        for idx, row in machine_weekends_df.iterrows():
            weekend_date = row['date']
            with logger.context(f"{weekend_date}"):
                logger.info(f"Started making interval and constraints for date: {weekend_date}")

                day_start_seconds = DTS(datetime.combine(weekend_date.date(), time.min), logger)
                logger.info(f"Start day in seconds from now: {day_start_seconds}")
                day_end_seconds = DTS(datetime.combine(weekend_date.date(), time(23, 59, 59)), logger)
                logger.info(f"Start end in seconds from now: {day_end_seconds}")

                start_before = model_data.model.NewBoolVar(f"start_before_weekend_{task_id}_{idx}")
                start_after = model_data.model.NewBoolVar(f"start_after_weekend_{task_id}_{idx}")
                logger.info("Created 2 bool intervals indicating if the task starts before or after the weekend")
                
                model_data.model.Add(start_var <= day_start_seconds - 1).OnlyEnforceIf(start_before)
                model_data.model.Add(start_var >= day_end_seconds + 1).OnlyEnforceIf(start_after)
                model_data.model.AddBoolOr([start_before, start_after]).WithName(f'start_not_in_weekend_{task_id}_{idx}')
                logger.info("Added constraint saying the task has to either start before or after the weekend")
                
                end_before = model_data.model.NewBoolVar(f"end_before_weekend_{task_id}_{idx}")
                end_after = model_data.model.NewBoolVar(f"end_after_weekend_{task_id}_{idx}")
                logger.info("Created 2 bool intervals indicating if the task ends before or after the weekend")

                model_data.model.Add(end_var <= day_start_seconds - 1).OnlyEnforceIf(end_before)
                model_data.model.Add(end_var >= day_end_seconds + 1).OnlyEnforceIf(end_after)
                model_data.model.AddBoolOr([end_before, end_after]).WithName(f'end_not_in_weekend_{task_id}_{idx}')
                logger.info("Added constraint saying the task has to either end before or after the weekend")
                
                weekend_inside_flag = model_data.model.NewBoolVar(f"weekend_inside_{task_id}_{idx}")
                model_data.model.AddBoolAnd([start_before, end_after]).OnlyEnforceIf(weekend_inside_flag)
                weekend_flags.append(weekend_inside_flag)
                logger.info("Created a boolean variable that is true if the task starts before and ends after the weekend")

                interval_data['weekend_flag_map'][weekend_date] = weekend_inside_flag
        
                individual_penalty = model_data.model.NewIntVar(0, penalty_per_weekend_day, f"weekend_extension{task_id}")
                model_data.model.Add(individual_penalty == weekend_inside_flag * penalty_per_weekend_day)
                logger.info("Created variable linked to weekend days inside task")

                weekend_penalty_vars.append(individual_penalty)
        
        logger.info("Finished looping through all the weekends")
        total_weekend_extension = model_data.model.NewIntVar(
            0, len(weekend_penalty_vars) * penalty_per_weekend_day, f"total_ext_{task_id}"
        )
        model_data.model.Add(total_weekend_extension == sum(weekend_penalty_vars))
        logger.info(f"Created total_weekend_extension variable and linked it to sum of potential penalties.")

        model_data.model.Add(end_var == start_var + duration + total_weekend_extension).WithName(f"final_end_{task_id}")
        logger.info("Added final constraint: end_var == start_var + duration + total_weekend_extension")

