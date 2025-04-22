from ortools.sat.python import cp_model
import logging
from datetime import datetime, timedelta
import pandas as pd
import os
from CPSAT.Domain.ScheduleDataClass import SchedulingModelData
from CPSAT.TaskVSTaskFunctions import add_task_vs_task_penalties
from CPSAT.ProcesResultFunctions import extract_machine_schedules

INFO_CODE_NONE = 0
INFO_CODE_MISSED_DEADLINE = 1
INFO_CODE_PAST_DEADLINE = 2
INFO_CODE_INTERRUPTED_EMERGENCY = 3

# Used for adjusting the upper limit of alot of variables at once
MAX_LIMIT_MULTIPLIER = 100

PREP_REASON_SUBSERIE_SWAP = "Subserie Swap"
PREP_REASON_IML_SWAP = "IML Swap"
PREP_REASON_BASIC_SWAP = "Basic Swap"
PREP_REASON_MAINTENANCE = "Maintenance"
PREP_REASON_EMERGENCY_MAINTENANCE = "Emergency Maintenance"


def create_optimized_schedule_df(possible_machines_for_orders_df, machine_names_df, maintenance_df):
    model_data = SchedulingModelData()
    model = model_data.model
    solver = model_data.solver
    
    machine_ids = machine_names_df['_k2_machineID'].unique()
    model_data.unique_order_ids = possible_machines_for_orders_df['orderID'].unique()
    model_data.machine_intervals = {machine_id: [] for machine_id in machine_ids}

    prepare_tasks_data(
        model_data, possible_machines_for_orders_df
    )

    add_all_constraints(model_data)

    model.Minimize(sum(model_data.all_penalties)) 
    status = solver.Solve(model)
    machine_schedules_df = extract_machine_schedules(
        model_data, solver, status
    )   
    return machine_schedules_df

def prepare_tasks_data(model_data, possible_machines_for_orders_df):
    past_deadline_vars = {}
    task_intervals = {}

    for order_id in model_data.unique_order_ids:
        order_rows_df = possible_machines_for_orders_df[possible_machines_for_orders_df['orderID'] == order_id]
        solutions_for_order = parse_possible_solutions_for_order(model_data, order_id, order_rows_df)

        create_intervals_for_order(solutions_for_order, model_data, past_deadline_vars, task_intervals)


def create_intervals_for_order(solutions_for_order, model_data, past_deadline_vars, task_intervals):
    machine_choices = []

    for task_option_index, task_details in enumerate(solutions_for_order):
        detailed_task_id = task_details['task_id']
        machine_id = task_details['machine_id']
        duration = task_details['duration']
        latest_start_possible_in_seconds_from_now = int(task_details['latest_start_possible_in_seconds_from_now'])
        start_var = model_data.model.NewIntVar(0, latest_start_possible_in_seconds_from_now * MAX_LIMIT_MULTIPLIER, f'start_{detailed_task_id}')
        duration_var = model_data.model.NewConstant(duration)
        end_var = model_data.model.NewIntVar(0, latest_start_possible_in_seconds_from_now * MAX_LIMIT_MULTIPLIER + duration, f'end_{detailed_task_id}')

        # Definieer een boolean die aantoont of de huidige machine gekozen is voor dat order of niet
        # Een boolean per task (mogelijk machine voor deze order)
        is_chosen = model_data.model.NewBoolVar(f'machine_choice_{detailed_task_id}')

        interval_var = model_data.model.NewOptionalIntervalVar(start_var, duration_var, end_var, is_chosen, f'{detailed_task_id}')

        # Verzamel al de booleans voor een specifieke order
        # Deze wordt gebruikt in de model.AddExactlyOne() later om te zeggen slechts een van deze machine choice var booleans mag True zijn.
        machine_choices.append(is_chosen)

        # Bewaart al de mogelijke tasks hun interval voor deze order
        task_intervals[(detailed_task_id)] = interval_var
        # De machine_intervals bevat elke mogelijke interval voor een machine met als key de machine id
        model_data.machine_intervals[machine_id].append({
            'interval': interval_var,
            'details': task_details,
            'is_chosen': is_chosen,
            'scheduled_start_time': start_var,
            'scheduled_end_time': end_var
            })

    #Constraint: add exactly one task from the possible machines for an order, not 2 not 0.
    model_data.model.AddExactlyOne(machine_choices)


def parse_possible_solutions_for_order(model_data, order_id, order_rows_df):
    solutions_for_order = []
    for index, machine_row in order_rows_df.iterrows():
        subserie_id = machine_row['subserieID']
        machine_id = machine_row['machineID']
        duration_seconds = int(machine_row['secondsNeeded'])
        dueDate = machine_row['dueDate']
        info = machine_row['info'] if pd.notna(machine_row['info']) else ''
        iml_possible = machine_row['IML possible']
        priority_code = machine_row['priority_code']
        article_id = machine_row['articleID']
        machine_status = machine_row['machine_status']
        maintenance_windows_input = machine_row['machine_maintenance_windows']

        #Add the maintenance windows for that machine to each possible task seperate
        maintenance_windows = []
        for window in maintenance_windows_input: # Process maintenance windows
            start_time_dt = datetime.strptime(window['start_time'], '%Y-%m-%d %H:%M:%S') # Parse time strings to datetime
            end_time_dt = datetime.strptime(window['end_time'], '%Y-%m-%d %H:%M:%S')

            start_seconds = int((start_time_dt - datetime.now()).total_seconds()) # Convert to seconds from now
            end_seconds = int((end_time_dt - datetime.now()).total_seconds())

            maintenance_windows.append({ # Append to maintenance windows array
                'start_seconds_from_now': max(0, start_seconds), # Ensure not negative
                'end_seconds_from_now': max(0, end_seconds) # Ensure not negative
            })


        latest_start_possible_in_seconds_from_now = calculate_latest_start_possible_in_seconds_from_now(dueDate, duration_seconds)
        is_past_due_date = False

        if dueDate and dueDate <= datetime.now():
            is_past_due_date = True

        task_id = f"{order_id}_{subserie_id}_{machine_id}_{iml_possible}"
        solutions_for_order.append({
            'task_id': task_id,
            'order_id': order_id,
            'subserie_id': subserie_id,
            'machine_id': machine_id,
            'duration': duration_seconds,
            'latest_start_possible_in_seconds_from_now': latest_start_possible_in_seconds_from_now,
            'original_due_date': dueDate,
            'original_latest_start_date_str': dueDate.strftime('%Y-%m-%d %H:%M:%S') if dueDate else None,
            'is_past_due_date': is_past_due_date,
            'priority_code': int(priority_code),
            'info': info,
            'iml_possible': iml_possible,
            'article_id': article_id,
            'machine_status': int(machine_status),
            'maintenance_windows': maintenance_windows
        })

        # Change the max deadline date so that it is always the latest deadline from all tasks, stock fill orders do not count since they have no deadline.
        model_data.max_ceiling_variable = max(model_data.max_ceiling_variable, (int((dueDate - datetime.now()).total_seconds()) * MAX_LIMIT_MULTIPLIER) if dueDate and priority_code != 7 else 0)
    return solutions_for_order


def calculate_latest_start_possible_in_seconds_from_now(due_date, duration_seconds):
    duration_timedelta = timedelta(seconds=duration_seconds)
    latest_start_date_possible = due_date - duration_timedelta if due_date else datetime.now() - duration_timedelta

    time_difference = latest_start_date_possible - datetime.now()
    latest_start_possible_in_seconds_from_now = time_difference.total_seconds()

    return max(0, latest_start_possible_in_seconds_from_now)



def add_no_overlap_constraint(model_data, machine_task_intervals, machine_id):
    machine_interval_vars_list = [
        data['interval']
        for data in machine_task_intervals
        if 'interval' in data and data['interval'] is not None
    ]
    model_data.model.AddNoOverlap(machine_interval_vars_list).WithName(f'no_overlap_constraint_machine_{machine_id}')

def add_deadline_slack_constraint(model_data, interval_data):
    due_date = interval_data['details']['original_due_date']
    task_id = interval_data['details']['task_id']

    if due_date > datetime.now():
        due_date_seconds = int((due_date - datetime.now()).total_seconds())

        deadline_slack_var = model_data.model.NewIntVar(0, due_date_seconds * MAX_LIMIT_MULTIPLIER, f'deadline_slack_{task_id}')
        model_data.model.Add(deadline_slack_var >= interval_data['scheduled_end_time'] - due_date_seconds).OnlyEnforceIf(interval_data['is_chosen']).WithName(f'interval_for_order_force_deadline_slack_larger_than_deadline_constraint_{task_id}')

        model_data.model.Add(deadline_slack_var == 0).OnlyEnforceIf(interval_data['is_chosen'].Not()).WithName(f'interval_for_order_force_deadline_slack_equal_to_0_constraint_{task_id}')

        interval_data['deadline_slack_var'] = deadline_slack_var

def add_makespan_constraint(model_data, interval_data):
    add_deadline_slack_constraint(model_data, interval_data)
    task_id = interval_data['details']['task_id']
    end_time = interval_data['scheduled_end_time']
    start_var = interval_data['scheduled_start_time']
    priority_code = interval_data['details']['priority_code']
    machine_id = interval_data['details']['machine_id']
    is_chosen = interval_data['is_chosen']
    deadline_slack = interval_data['deadline_slack_var']


    model_data.end_vars.append(end_time)

    # Emergency order, plan right now take any machine no matter what to minimize the end var, largest punishment scaling with end var to the point where nothing else matters if this is not optimised.
    if priority_code == 1:
        model_data.model.Add(start_var <= 1).WithName(f'makespan_constraint_emergency_order_force_start_var_below_1_{task_id}')
        model_data.all_penalties.append(end_time * 10000)

        # Keep track of what machine is used for the emergency, needed for prio 2 situation
        model_data.emergency_used_machine_ids.append(machine_id)

    # Track of what machines were currently running with what order
    elif priority_code == 2:
        model_data.currently_running_tasks.append(interval_data)

    # The order is a priority however it is not allowed to interrupt any currently running orders
    elif priority_code == 3:
        # Define a domain
        asap_no_interrupt_penalty_var = model_data.model.NewIntVar(0, model_data.model.max_ceiling_variable , f'asap_no_interrupt_penalty_{task_id}')

        # If this task is chosen then the asap_no_interrupt_penalty_var is equal to the end time of that task
        model_data.model.Add(asap_no_interrupt_penalty_var == end_time).OnlyEnforceIf(is_chosen).WithName(f'makespan_constraint_emergency_order_force_start_var_below_1_{task_id}')

        # Define a penalty scaling with how late this order starts, making the algorithm prioritise this order
        # Make the punishment larger than other penalties
        model_data.all_penalties.append(asap_no_interrupt_penalty_var * 50)

    # Prio code mag deadline absoluut niet missen: grote penalty voor missen
    elif priority_code == 4:
        model_data.all_penalties.append(deadline_slack * 5)
        
    # Prio code normale order: probeer deadline niet te missen maar geen ramp: kleine penalty
    elif priority_code == 5:
        model_data.all_penalties.append(deadline_slack * 1)

    # Loop through every single order that was currently running and either put them first or apply punishment
    for interval_data in model_data.currently_running_tasks:
        task_id = interval_data['details']['task_id']
        machine_id = interval_data['details']['machine_id']
        if machine_id not in model_data.emergency_used_machine_ids:
                # Force start time to 0 only if no priority 1 on same machine
            model_data.model.Add(start_var == 0).WithName(f'makespan_constraint_emergency_order_force_start_var_below_1_{task_id}')
        else:
            # If priority 1 order uses this machine, apply a large punishment to finish them as soon as possible but not large enough to take prio over the emergency
            emergency_and_running_end_time_penalty_sum += end_time * 1000


def add_all_constraints(model_data):
    for machine_id, machine_task_intervals in model_data.machine_intervals.items():
        add_force_proper_order_constraint(model_data, machine_task_intervals, machine_id)
        add_no_overlap_constraint(model_data, machine_task_intervals, machine_id)
        for interval_data in machine_task_intervals:
            add_task_vs_task_penalties(model_data, interval_data, machine_task_intervals, machine_id)
            add_maintenance_window_constraints(model_data, interval_data, machine_id)
            add_makespan_constraint(model_data, interval_data)

    # Finally define the end goal if making the latest end time (makespan) plus all of the penalties as small as possible
    total_end_sum = model_data.model.NewIntVar(0, model_data.max_ceiling_variable * len(model_data.end_vars), 'total_end_sum')
    model_data.model.Add(total_end_sum == sum(model_data.end_vars))
    model_data.all_penalties.append(total_end_sum)

def add_maintenance_window_constraints(model_data, interval_data, machine_id):
    maintenance_windows = interval_data['details']['maintenance_windows']
    is_chosen = interval_data['is_chosen']
    task_id = interval_data['details']['task_id']
    interval_var = interval_data['interval']
    for window_index, window in enumerate(maintenance_windows):
        maintenance_status = window['status']
        maintenance_start_seconds = window['start_seconds_from_now']
        maintenance_end_seconds = window['end_seconds_from_now']

        # Een optional var die niet altijd deel is van de uitkomst met een vaste duur die niet aangepast kan worden
        # Krijgt een start tijd, duur, eind tijd en voorwaarde dat het enkel aanwezig is als de machine niet gekozen is, en een naam
        # Start and end times for the maintenance window are fixed variables so the model cannot change these to prevent overlap ==> good
        maintenance_interval = model_data.model.NewOptionalFixedSizeIntervalVar(maintenance_start_seconds, maintenance_end_seconds - maintenance_start_seconds, maintenance_end_seconds, is_chosen.Not(), f'maintenance_interval_{task_id}_{window_index}')
        overlap_var = model_data.model.NewBoolVar(f'maintenance_overlap_{task_id}_{window_index}')

        # Detects if overlap occurs between the interval of the task and the maintenance interval
        model_data.model.Add(model_data.model.Overlap(interval_var, maintenance_interval)).OnlyEnforceIf(overlap_var).WithName(f'machine_status_constraint_no_overlap_maintenance_force_boolean_task_id:{task_id}_machine_id:{machine_id}')

        # Emergency Maintenance: Machine unavailable during window
        if maintenance_status == 1:
            # If overlap is true then force the machine to not be chosen by making the chosen boolean false
            model_data.model.AddImplication(overlap_var, is_chosen == 0).WithName(f'machine_status_constraint_no_overlap_maintenance_status_1_task_id:{task_id}_machine_id:{machine_id}')

        # It is prefered machine is not running at that moment but can move slightly when it optimizes the schedule alot
        elif maintenance_status == 2:
            maintenance_start_var = model_data.model.NewIntVar(0, model_data.max_ceiling_variable , f'maintenance_start_{task_id}_{window_index}')
            maintenance_shift_penalty_var = model_data.model.NewIntVar(0, model_data.max_ceiling_variable, f'maintenance_shift_penalty_{task_id}_{window_index}')
            shift_duration_var = model_data.model.NewIntVar(0, model_data.max_ceiling_variable , f'shift_duration_{task_id}_{window_index}')

            model_data.model.Add(shift_duration_var == cp_model.Abs(maintenance_start_var - maintenance_start_seconds)).OnlyEnforceIf(is_chosen).WithName(f'machine_status_constraint_no_overlap_maintenance_status_2_calculate_difference_task_id:{task_id}_machine_id:{machine_id}')
            model_data.model.Add(maintenance_shift_penalty_var == shift_duration_var * 50).WithName(f'machine_status_constraint_no_overlap_maintenance_status_2_define_penalty_task_id:{task_id}_machine_id:{machine_id}')

            model_data.all_penalties.append(maintenance_shift_penalty_var)


def add_force_proper_order_constraint(model_data, machine_task_intervals, machine_id):
    chosen_list = []

    chosen_count = model_data.model.NewIntVar(0, len(machine_task_intervals), f"chosen_count_{machine_id}")

    for interval_data in machine_task_intervals:
        task_interval_id = interval_data['details']['task_id']
        chosen_list.append(interval_data['is_chosen'])

        # Every task can have an order number between 0 and the number of tasks possible on the machine
        interval_data["order_var"] = model_data.model.NewIntVar(-1, len(machine_task_intervals) - 1, f"order_{task_interval_id}")

        # Every task that is not chosen gets order position -1
        model_data.model.Add(interval_data["order_var"] == -1).OnlyEnforceIf(interval_data['is_chosen'].Not())

        model_data.model.Add(interval_data["order_var"] < chosen_count).OnlyEnforceIf(interval_data['is_chosen'])
        model_data.model.Add(interval_data["order_var"] >= 0).OnlyEnforceIf(interval_data['is_chosen'])

    # Force that the chosen_count var has to be equal to the amount of tasks on the machine that are chosen
    model_data.model.Add(chosen_count == sum(chosen_list))
