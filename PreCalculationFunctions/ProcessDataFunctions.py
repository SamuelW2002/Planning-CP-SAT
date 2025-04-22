from datetime import date, datetime, timedelta
import pandas as pd
import uuid
from Domain.PossibleTask import PossibleTask
from MongoDBFunctions.MongoQuerries import get_machine_unavailable_windows, get_machine_unavailable_from_batch

MAX_DUE_DATE = 15778800

def calculate_production_plan(dataframes, logger):
    with logger.context("Splitting Orders Into Tasks"):
        logger.info("Started looking up every possible machine for an order")

        processed_data_df = dataframes.processed_data_df
        all_orders_df = dataframes.all_orders_df
        all_possible_tasks_list = []
        logger.info("Looping through all of the orders")
        for index, order_row in all_orders_df.iterrows():
            subserie_id = order_row["subserieID"]
            with logger.context(f"Processing row for order with subserie ID: {subserie_id}"):
                logger.info(f"Started processing order row")
                iml_requested = bool(order_row['iml'])
                orderID_uuid = uuid.uuid4()
                orderID_str = str(orderID_uuid)

                logger.info("Finding all machines that are capable of making this subserie")
                possible_machines_df = processed_data_df[
                    (processed_data_df['subserieID'] == int(subserie_id))
                ]
                if possible_machines_df.empty:
                    logger.info(f"No machines capable of making subserie with ID: {subserie_id}")
                    logger.feedback(f"No machines capable of making subserie with ID: {subserie_id}")
                    continue

                iml1_machines_exist = not possible_machines_df[possible_machines_df['IML'] == 1].empty
                filtered_machines_for_iml = possible_machines_df

                if iml_requested and iml1_machines_exist:
                    logger.info("IML was requested and machines were found capable of making this subserie with IML")
                    logger.info("deleting machines where subserie cannot be made with IML")
                    filtered_machines_for_iml = possible_machines_df[possible_machines_df['IML'] == 1]
                    logger.addDataFrameRecords("Deleted machines", possible_machines_df[possible_machines_df['IML'] == 0])
                    logger.addDataFrameRecords("Viable machines", possible_machines_df[possible_machines_df['IML'] == 1])


                if iml_requested and not iml1_machines_exist:
                    logger.info(f"IML was requested however no machines are capable of making subserie with ID: {subserie_id} with an IML")
                    logger.feedback(f"IML was requested however no machines are capable of making subserie with ID: {subserie_id} with an IML")
                    continue
                
                logger.info("Looping through all viable machines and creating tasks")
                for index_machine, machine_row in filtered_machines_for_iml.iterrows():
                    task = PossibleTask(order_row=order_row, machine_row=machine_row, orderID_str=orderID_str)
                    with logger.context(f"Started creating task for subserie with ID: {subserie_id}, machine with ID: {task.machineID} and IML value: {iml_requested}"):
                        earliest_finish_time = datetime.now() + timedelta(seconds=task.secondsNeeded)
                        logger.info(f"Producing this order on this machine will take {task.secondsNeeded} seconds")
                        logger.info(f"Earliest time possible to finish this task on this machine is: {earliest_finish_time}")

                        if task.dueDate < earliest_finish_time:
                            logger.info(f"Due date {task.dueDate.strftime('%Y-%m-%d')} cannot be met, scheduler will prioritize this task by using deadline slack")
                            task.info_messages.append(f"Due date {task.dueDate.strftime('%Y-%m-%d')} cannot be met, scheduler will prioritize this task")

                        if task.default_cycle_avg_used:
                            logger.info('No data... A cycle average of 10 seconds was used...')
                            task.info_messages.append('No data... A cycle average of 10 seconds was used...')

                        if task.default_cav_used and not task.default_cycle_avg_used:
                            logger.info('No cavity amount... used default cav amount of 4...')
                            task.info_messages.append('No cavity amount... used default cav amount of 4...')

                        non_iml_data_exists = not possible_machines_df[
                            (possible_machines_df['machineID'] == task.machineID) &
                            (possible_machines_df['IML'] == 0) &
                            (possible_machines_df['cycleAvg'] != 10)
                        ].empty

                        if not task.iml_requested and task.iml_possible_machine and not non_iml_data_exists:
                            logger.info('No IML was requested but this machine can create the subserie with an IML, there was no data on production without an IML')
                            task.info_messages.append('Production might be faster... no IML requested but avgCycle for with IML used, no data for no IML available')

                        if task.default_cycle_avg_used and task.iml_requested:
                            logger.info(f'No data... for subserie {task.subserieID} on machine {task.machineID} with IML... 10s used.')
                            task.info_messages.append(f'No data... for subserie {task.subserieID} on machine {task.machineID} with IML... 10s used.')
                            if non_iml_data_exists:
                                logger.info(f'However, non-IML data exists for this machine.')
                                task.info_messages.append(f'However, non-IML data exists for this machine.')

                        processed_orders_dict = task.to_dict()
                        processed_orders_dict['info'] = ' '.join(task.info_messages)
                        all_possible_tasks_list.append(processed_orders_dict)
        df = pd.DataFrame(all_possible_tasks_list)
        logger.info("Finished looping through all of the orders")
        return df

def calculate_unavailable_weekends(dataframes, logger) -> pd.DataFrame:
    try:
        with logger.context("Unavailable Weekends"):
            logger.info("Calculating all weekends that are not available for production")
            machine_names_df = dataframes.machine_names_df
            available_weekends_df = dataframes.available_weekends_df
            required_final_cols = ['resourceId', 'date']
            today = date.today()

            latest_leverdatum_date = today + timedelta(seconds=MAX_DUE_DATE)
            logger.info(f"Set latest possible date at half a year from now: {latest_leverdatum_date}")

            all_dates_in_range = pd.date_range(start=today, end=latest_leverdatum_date, freq='D')
            logger.info("Generated dates for every date in range")

            all_weekend_dates = all_dates_in_range[all_dates_in_range.dayofweek >= 5].normalize()
            logger.info("Deleted every date that is not Saturday or Sunday")

            machine_ids = machine_names_df['_k2_machineID'].unique()
            multi_index = pd.MultiIndex.from_product([machine_ids, all_weekend_dates], names=['machineId', 'date'])
            all_possible_unavailable = pd.DataFrame(index=multi_index).reset_index()
            logger.info("Created record for every date for every machine")

            if not available_weekends_df.empty:
                print("test")
                available_weekends_df['date'] = pd.to_datetime(available_weekends_df['date'], errors='coerce').dt.normalize()
                logger.info("Retrieved DataFrame for all weekends that ARE available")
                merged_df = pd.merge(
                    all_possible_unavailable,
                    available_weekends_df,
                    on=['resourceId', 'date'],
                    how='left',
                    indicator=True
                )
                unavailable_weekends_df = merged_df[merged_df['_merge'] == 'left_only'][required_final_cols]
                logger.addDataFrameRecords("Removed weekends that are available", unavailable_weekends_df)
            else:
                logger.info("No available weekend data provided or valid. All generated weekends considered unavailable.")
                unavailable_weekends_df = all_possible_unavailable
            unavailable_weekends_df = unavailable_weekends_df.sort_values(by=['machineId', 'date']).reset_index(drop=True)

            logger.info(f"Final calculation: {len(unavailable_weekends_df)} unavailable machine/weekend slots identified.")
            return unavailable_weekends_df
    except Exception as e:
        logger.error(f"Error calculating unavailable weekends: {e}")
        return pd.DataFrame()
    
def get_machine_unavailable_df(db_manager, logger):
    try:
        with logger.context("Processing Machine Unavailable Timespans"):
            final_columns = ['resourceId', 'startDate', 'endDate']
            combined_df = pd.DataFrame(columns=final_columns)

            df_maintenance = get_machine_unavailable_windows(db_manager, logger)
            df_batch = get_machine_unavailable_from_batch(db_manager, logger)
            logger.info("Finished retrieving data from MongoDB")

            if not df_maintenance.empty:
                if 'machineId' in df_maintenance.columns:
                    df_maintenance = df_maintenance.rename(columns={'machineId': 'resourceId'})
                    logger.info("Parsed machineId column to resourceId in maintenance DataFrame")
                elif 'resourceId' not in df_maintenance.columns:
                    logger.error("Warning: Neither 'machineId' nor 'resourceId' found in maintenance data. Setting resourceId to NA.")
                    df_maintenance['resourceId'] = pd.NA

                try:
                    if not pd.api.types.is_datetime64_any_dtype(df_maintenance['startDate']):
                        logger.error("Warning: startDate in maintenance df is not datetime. Attempting conversion.")
                        df_maintenance['startDate'] = pd.to_datetime(df_maintenance['startDate'], errors='coerce')
                    if not pd.api.types.is_datetime64_any_dtype(df_maintenance['endDate']):
                        logger.error("Warning: endDate in maintenance df is not datetime. Attempting conversion.")
                        df_maintenance['endDate'] = pd.to_datetime(df_maintenance['endDate'], errors='coerce')

                    df_maintenance = df_maintenance[final_columns].copy()

                    logger.addDataFrameRecords(f"Finished checking and parsing data from maintenance DataFrame and appended {len(df_maintenance)} records to final DataFrame", df_maintenance)
                except KeyError as e:
                    logger.error(f"Missing expected column {e} in maintenance data. Skipping.")
                except Exception as e:
                    logger.error(f"Error processing maintenance data: {e}. Skipping.")


            if not df_batch.empty:
                if 'resourceId' not in df_batch.columns:
                    logger.error("Warning: 'resourceId' not found in batch data. Setting resourceId to NA.")
                    df_batch['resourceId'] = pd.NA

                try:
                    if not pd.api.types.is_datetime64_any_dtype(df_batch['startDate']):
                        logger.error("Warning: startDate in batch df is not datetime. Attempting conversion.")
                        df_batch['startDate'] = pd.to_datetime(df_batch['startDate'], errors='coerce')
                    if not pd.api.types.is_datetime64_any_dtype(df_batch['endDate']):
                        logger.error("Warning: endDate in batch df is not datetime. Attempting conversion.")
                        df_batch['endDate'] = pd.to_datetime(df_batch['endDate'], errors='coerce')

                    df_batch = df_batch[final_columns].copy()

                    logger.addDataFrameRecords(f"Finished checking and parsing data from batches DataFrame and appended {len(df_batch)} records to the final DataFrame", df_batch)
                except KeyError as e:
                    logger.error(f"Missing expected column {e} in batch data. Skipping.")
                except Exception as e:
                    logger.error(f"Error processing batch data: {e}. Skipping.")


            if df_maintenance.empty and df_batch.empty:
                logger.info("No machine unavailable data was found. Returning empty DataFrame.")
                return combined_df

            combined_df = pd.concat([df_maintenance, df_batch], ignore_index=True)
            logger.info(f"Combined DataFrame has {len(combined_df)} rows before final date validation.")

            if not combined_df.empty:
                start_invalid_mask = combined_df['startDate'].isna()
                end_invalid_mask = combined_df['endDate'].isna()
                any_invalid_date_mask = start_invalid_mask | end_invalid_mask

                invalid_rows_df = combined_df[any_invalid_date_mask]

                if not invalid_rows_df.empty:
                    with logger.context("Discarding invalid dates"):
                        logger.addDataFrameRecords(f"Found {len(invalid_rows_df)} rows with invalid dates (NaT). Logging and discarding...", invalid_rows_df)
                        for row in invalid_rows_df.itertuples(index=False):
                            logger.info(
                                f"Discarding entry due to invalid date(s): "
                                f"resourceId={getattr(row, 'resourceId', 'N/A')}, "
                                f"startDate='{getattr(row, 'startDate', 'N/A')}', "
                                f"endDate='{getattr(row, 'endDate', 'N/A')}'"
                            )

                valid_df = combined_df[~any_invalid_date_mask].copy()
                initial_rows_valid_dates = len(valid_df)
                logger.info(f"Found {initial_rows_valid_dates} records with valid dates")

                valid_df.dropna(subset=['resourceId'], inplace=True)
                dropped_resource_count = initial_rows_valid_dates - len(valid_df)
                if dropped_resource_count > 0:
                    logger.info(f"Discarded {dropped_resource_count} additional rows due to missing resourceId.")

                logger.addDataFrameRecords(f"Returning final combined DataFrame with {len(valid_df)} valid unavailability entries", valid_df)
                return valid_df
            else:
                logger.info("Combined DataFrame is empty before date validation.")
                return combined_df

    except Exception as e:
        logger.error(f"Error during combination/validation of unavailability data: {e}")
        return pd.DataFrame(columns=final_columns)