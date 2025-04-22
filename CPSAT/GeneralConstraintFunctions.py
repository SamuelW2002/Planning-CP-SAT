
from datetime import datetime, time
from HelperFunctions.HelperFunctions import DTS


def add_general_constraints(model_data, subserie_swap_cap, dataframes, logger):
    with logger.context("General Constraints"):
        logger.info("Adding all the constraints that apply over everything")

        all_cumulative_intervals = (model_data.cp_subserie_swap_interval_vars +
                                    model_data.cp_capacity_reduction_intervals)
        logger.info("Created a list containing all of the actual potential intervals and all of the reduction block intervals")

        all_cumulative_demands = ([1] * len(model_data.cp_subserie_swap_interval_vars) +
                                model_data.cp_capacity_reduction_demands)
        logger.info("Created a list of 1's n times where n is the amount of actual potential intervals and then the demands so example: [1,1,1,1,1,..., 2 , 2 ,2]")

        logger.info(f"Adding Cumulative constraint for preparations with capacity {subserie_swap_cap}")
        # Amount of items in the all_cumulative_intervals and all_cumulative_demands need to match up because they will be linked on placement in the list
        # The AddCumulative will check if the fixed intervals inside the list are active and if they are their corresponding demands cannot be larger than 3
        # This means that if a worker is not present the demand for that timeblock is 1 and only 2 intervals can be active at once during that time period.
        model_data.model.AddCumulative(
            intervals=all_cumulative_intervals,
            demands=all_cumulative_demands,
            capacity=subserie_swap_cap
        ).WithName("max_concurrent_preparations_varying")
        logger.info("Successfully added preparation cumulative constraint.")
        
        logger.info(f"Applying weekend prohibition constraints to {len(model_data.cp_IML_swap_intervals)} IML swaps.")
        for prep_machine_dict in model_data.cp_IML_swap_intervals:
            add_weekend_constraints_IML_swaps(model_data, prep_machine_dict, dataframes, logger)


def add_weekend_constraints_IML_swaps(model_data, prep_machine_dict, dataframes, logger):
    weekends_unavailable_df = dataframes.weekends_unavailable_df
    if weekends_unavailable_df.empty:
        return

    machine_id = prep_machine_dict.get('machine_id')
    prep_interval = prep_machine_dict.get('prep_interval')

    start_var = prep_interval.StartExpr()
    end_var = prep_interval.EndExpr()

    machine_weekends_df = weekends_unavailable_df[
        weekends_unavailable_df['machineId'] == machine_id
    ]

    for idx, row in machine_weekends_df.iterrows():
        weekend_date = row['date']

        day_start_seconds = DTS(datetime.combine(weekend_date.date(), time.min), logger)
        day_end_seconds = DTS(datetime.combine(weekend_date.date(), time(23, 59, 59)), logger)

        start_before = model_data.model.NewBoolVar(f"start_before_weekend_{str(prep_interval)}_{idx}")
        start_after = model_data.model.NewBoolVar(f"start_after_weekend_{str(prep_interval)}_{idx}")
        
        model_data.model.Add(start_var <= day_start_seconds - 1).OnlyEnforceIf(start_before)
        model_data.model.Add(start_var >= day_end_seconds + 1).OnlyEnforceIf(start_after)
        model_data.model.AddBoolOr([start_before, start_after]).WithName(f'start_not_in_weekend_{str(prep_interval)}_{idx}')
        
        end_before = model_data.model.NewBoolVar(f"end_before_weekend_{str(prep_interval)}_{idx}")
        end_after = model_data.model.NewBoolVar(f"end_after_weekend_{str(prep_interval)}_{idx}")

        model_data.model.Add(end_var <= day_start_seconds - 1).OnlyEnforceIf(end_before)
        model_data.model.Add(end_var >= day_end_seconds + 1).OnlyEnforceIf(end_after)
        model_data.model.AddBoolOr([end_before, end_after]).WithName(f'end_not_in_weekend_{str(prep_interval)}_{idx}')