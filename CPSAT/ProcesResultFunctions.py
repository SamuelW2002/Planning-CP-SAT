from ortools.sat.python import cp_model
import logging
from datetime import datetime, timedelta
import pandas as pd


def extract_machine_schedules(model_data, solver, status, logger):
    with logger.context("Extracting Results"):
        logger.info("Extracting and processing final results from solver")
        if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
            logger.info(f"Solver status: {'Optimal' if status == cp_model.OPTIMAL else 'Feasible'}")
            current_time = datetime.now()
            
            logger.info("Extracting all of the planned tasks from the solver")
            scheduled_machines_list = []
            for machine in model_data.machine_intervals:
                with logger.context(f"Machine: {machine}"):
                    logger.info("Extracting all the chosen tasks for this machine")
                    possible_tasks_for_machine = model_data.machine_intervals.get(machine)
                    for task in possible_tasks_for_machine:
                        if solver.BooleanValue(task['is_chosen']):
                            task_id = task["task_id"]
                            with logger.context(f"Task: {task_id}"):
                                scheduled_start_seconds = solver.Value(task['interval'].StartExpr())
                                scheduled_end_seconds = solver.Value(task['interval'].EndExpr())
                                scheduled_start_time = current_time + timedelta(seconds=scheduled_start_seconds)
                                scheduled_end_time = current_time + timedelta(seconds=scheduled_end_seconds)
                                logger.info(f"This task was chosen for this machine to run between {scheduled_start_time} and {scheduled_end_time}")

                                weekend_flag_map = task.get('weekend_flag_map', {})
                                weekends_inside_task = []
                                for weekend_date, flag_variable in weekend_flag_map.items():
                                    if solver.Value(flag_variable) == 1:
                                        weekends_inside_task.append(weekend_date)
                                
                                original_due_date = task['due_date']
                                priority_code = task['priority_code']
                                info = task['info']
                                subserie_id = task['subserie_id']
                                machine_id = task['machine_id']
                                iml_possible = task['iml_possible']
                                duration = int(task['duration'])
                                duration_hour = duration / 3600

                                info_code = 0

                                if original_due_date:
                                    if task['impossible_deadline']:
                                        info_code = 1
                                    elif task['is_past_due_date']:
                                        info_code = 2

                                scheduled_order_details = {
                                    'mongoID': task['mongo_id'],
                                    'duration': duration_hour,
                                    'machineID': machine_id,
                                    'subserieID': subserie_id,
                                    'startTime': scheduled_start_time.strftime('%Y-%m-%d %H:%M:%S'),
                                    'endTime': scheduled_end_time.strftime('%Y-%m-%d %H:%M:%S'),
                                    'IML': iml_possible,
                                    'info_code': info_code,
                                    'extra_info': info,
                                    'preparation_or_maintenance': False,
                                    'weekends_inside': weekends_inside_task
                                }
                                scheduled_machines_list.append(scheduled_order_details)
            
            logger.info("Extracting preparation intervals from solver")
            no_production_intervals_list = []
            for preparation in model_data.setup_penalty_intervals:
                is_present_bool = preparation.get('enforce_bool_var')
                if solver.BooleanValue(is_present_bool):
                    penalty_start_var = preparation.get('penalty_start_var')
                    penalty_duration = preparation.get('penalty_duration')

                    scheduled_start_seconds = solver.Value(penalty_start_var)
                    scheduled_end_seconds = scheduled_start_seconds + penalty_duration
                    scheduled_start_time = current_time + timedelta(seconds=scheduled_start_seconds)
                    scheduled_end_time = current_time + timedelta(seconds=scheduled_end_seconds)
                    machine_id = preparation.get('machine_id')


                    duration = int(preparation.get('penalty_duration'))
                    duration_hour = duration / 3600

                    # Not yet used in frontend
                    task_1_description = preparation.get("task_1_description")
                    task_2_description = preparation.get("task_2_description")
                    task_1_matrijs = preparation.get("task_1_matrijs")
                    task_2_matrijs = preparation.get("task_2_matrijs")
                    task_1_hotrunner = preparation.get("task_1_hotrunner")
                    task_2_hotrunner = preparation.get("task_2_hotrunner")

                    penalty_type = preparation.get('penalty_type')
                    reason = f'{penalty_type.capitalize()} between 2 subseries:\n\nOrder 1 description: {task_1_description}\n\nOrder 2 description: {task_2_description}, en met MongoID {preparation.get("mongo_id")}'

                    setup_penalty_details = {
                        'mongoID': preparation.get('mongo_id'),
                        'duration': duration_hour,
                        'machineID': machine_id,
                        'articleID': None,
                        'subserieID': None,
                        'startTime': scheduled_start_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'endTime': scheduled_end_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'IML': None,
                        'info_code': None,
                        'extra_info': f"{penalty_type.capitalize()} Swap Penalty",
                        'preparation_or_maintenance': True,
                        'reason': reason,
                        'possible_machines_for_subserie': None,
                        'type': penalty_type,

                        # Not yet used in frontend
                        'task_1_description': task_1_description,
                        'task_2_description': task_2_description,
                        'task_1_matrijs': task_1_matrijs,
                        'task_2_matrijs': task_2_matrijs,
                        'task_1_hotrunner': task_1_hotrunner,
                        'task_2_hotrunner': task_2_hotrunner
                    }
                    no_production_intervals_list.append(setup_penalty_details)

            machine_schedules_df = pd.DataFrame(scheduled_machines_list)
            no_production_intervals_df = pd.DataFrame(no_production_intervals_list)
            machine_schedules_df = machine_schedules_df.sort_values(by=['machineID', 'startTime']).reset_index(drop=True) # Sort by startTime
            return machine_schedules_df, no_production_intervals_df

        elif status == cp_model.INFEASIBLE:
            logger.error("No feasible schedule found (INFEASIBLE).")
            return pd.DataFrame(), pd.DataFrame()
        else:
            logger.error(f"Solver status: {solver.StatusName(status)}")
            return pd.DataFrame(), pd.DataFrame()