from datetime import date, datetime, time, timedelta
import re
import pandas as pd
from HelperFunctions.HelperFunctions import checkMissingColumns, list_to_df
from MongoDBFunctions.MongoPipelines import all_technicians_pipeline, all_orders_pipeline, machine_unavailable_pipeline

def get_all_cycle_data(db_manager, logger):
    try:
        with logger.context("Average Cycle Time"):
            logger.info("Retrieving and processing all average cycle time data from MongoDB")
            projection = {
                "_id": 0,
                "subserieID": 1,
                "machineID": 1,
                "IML": 1,
                "cav": 1,
                "cycleAvg": 1
            }
            return mongo_query(None, projection, db_manager.avg_cycle_times, logger)
    except Exception as e:
        logger.error(f"Error fetching MongoDB data to DataFrame: {e}")
        return pd.DataFrame()
    
def get_all_orders(db_manager, logger):
    try:
        with logger.context("All Open Orders"):
            logger.info("Retrieving all orders from MongoDB and processing into DataFrame")
            pipeline = all_orders_pipeline()
            cursor = db_manager.batches_to_plan.aggregate(pipeline)

            list_cursor = list(cursor)
            if not list_cursor:
                logger.info(f"No documents found in MongoDB collection matching filter")
                return pd.DataFrame()
            logger.info("Retrieved all data from MongoDB")

            df = pd.DataFrame(list_cursor)
            if df.empty:
                logger.info("MongoDB query returned data, but resulted in an empty DataFrame")
                return pd.DataFrame()
            logger.info("Converted retrieved documents into DataFrame")

            logger.info(f"Attempting to parse leverdatum column to datetime...")
            original_count = len(df)
            original_leverdatums = df[["leverDatum"]].copy()
            df["leverDatum"] = pd.to_datetime(df["leverDatum"], errors='coerce')

            failed_mask = df["leverDatum"].isna()
            failed_rows = df[failed_mask]

            if not failed_rows.empty:
                num_failed = len(failed_rows)
                logger.info(f"Discarding {num_failed} out of {original_count} rows due to unparseable leverDatum")
                try:
                    failed_original_values = original_leverdatums[failed_mask]
                    for index, row_val in failed_original_values.iterrows():
                        logger.info(f'Row index {index}: Original leverDatum={row_val["leverDatum"]}')
                        logger.feedback(f'Order cannot be placed because leverDatum is not a valid date: {row_val["leverDatum"]}')
                except Exception as log_ex:
                    logger.error(f"Could not log specific failed leverDatum values: {log_ex}")

                df = df.dropna(subset=["leverDatum"])
                logger.info(f"Kept {len(df)} rows after leverDatum parsing check.")
            else:
                logger.info(f"All {original_count} rows have parseable leverDatum.")
            return checkMissingColumns(df, logger)
    except Exception as e:
        logger.error(f"Error during MongoDB aggregation/fetch to DataFrame: {e}")
        return pd.DataFrame()

def get_all_technicians(db_manager, logger):
    try:
        with logger.context("Technician Unavailability"):
            logger.info("Retrieving and processing all technicians available data")
            pipeline = all_technicians_pipeline()
            df = mongo_pipeline(pipeline, db_manager.batches_to_plan, logger)
            date_ranges = []
            for _, row in df.iterrows():
                start_date = row['startDate']
                end_date = row['endDate']
                ombouwers = row['ombouwersBeschikbaar']
                if start_date < pd.Timestamp(date.today()):
                    current_date = pd.Timestamp(date.today())
                else:
                    current_date = start_date
                while current_date <= end_date:
                    date_ranges.append({'date': current_date, 'ombouwersBeschikbaar': ombouwers})
                    current_date += timedelta(days=1)

            date_df = pd.DataFrame(date_ranges)
            logger.addDataFrameRecords("Turned date ranges into individual dates", date_df)

            logger.info("Parsed dates in DataFrame to Pandas date format")

            # Group by 'date' and find the minimum 'ombouwersBeschikbaar'
            result_df = date_df.groupby('date')['ombouwersBeschikbaar'].min().reset_index()
            logger.addDataFrameRecords("Deleted duplicate dates and took lowest technicians available value", result_df)

            logger.info(f"Found {len(result_df)} date entries where there is a change in ombouwersbeschikbaar.")
            return result_df

    except Exception as e:
        logger.error(f"Error during MongoDB aggregation/fetch to DataFrame: {e}")
        return pd.DataFrame()
    
def get_available_weekends(db_manager, logger):
    try:
        with logger.context("Available Weekends"):
            logger.info("Retrieving all weekends that are available for production")
            today_start_dt = datetime.combine(date.today(), time.min)

            query_filter = {
                'startDate': {
                    '$gte': today_start_dt
                }
            }
            projection = {
                '_id': 0,
                'resourceId': 1,
                'startDate': 1,
                'duration': 1,
                'durationUnit': 1
            }
            df = mongo_query(query_filter, projection, db_manager.resource_time_ranges, logger)
            if df.empty:
                return pd.DataFrame()
            
            df_day1 = df[['resourceId', 'startDate']].copy()
            df_day1.rename(columns={'startDate': 'date'}, inplace=True)
            logger.addDataFrameRecords("Created a copy DataFrame indicating the start day of a 'weekend'", df_day1)

            df_needs_day2 = df[(df['duration'] == 2) & (df['durationUnit'] == 'd')].copy()

            if not df_needs_day2.empty:
                logger.addDataFrameRecords(f"Found {len(df_needs_day2)} records that have a duration of 2 days", df_needs_day2)
                df_day2 = df_needs_day2[['resourceId', 'startDate']].copy()
                df_day2['date'] = df_day2['startDate'] + timedelta(days=1)
                df_day2 = df_day2[['resourceId', 'date']]

                result_df = pd.concat([df_day1, df_day2], ignore_index=True)

            result_df.sort_values(by=['resourceId', 'date'], inplace=True)
            logger.addDataFrameRecords(f"Final DataFrame after processing", result_df)

            logger.info(f"Successfully transformed data into {len(result_df)} resource/date entries.")
            return result_df
    except Exception as e:
        logger.error(f"Error processing MongoDB weekend data: {e}")
        return pd.DataFrame()
    
def get_machine_unavailable_windows(db_manager, logger):
    try:
        with logger.context("Machine Unavailability Onderhoud"):
            logger.info("Retrieving machine maintenance windows from the Onderhoud collection")
            query_filter = {
                'startDate': {'$exists': True, '$ne': None},
                'endDate': {'$exists': True, '$ne': None}
            }
            projection = {
                '_id': 0, 'startDate': 1, 'endDate': 1, 'machineId': 1
            }
            return mongo_query(query_filter, projection, db_manager.machines_unavailable, logger)
    except Exception as e:
        logger.error(f"Error processing MongoDB machine maintenance from onderhoud data: {e}")
        return pd.DataFrame()

def get_machine_unavailable_from_batch(db_manager, logger):
    try:
        with logger.context("Machine Unavailability Batch"):
            logger.info("Retrieving all machine maintenance windows from the batches collection")
            pipeline = machine_unavailable_pipeline()
            return mongo_pipeline(pipeline, db_manager.batches_to_plan, logger)
    except Exception as e:
        logger.error(f"Error processing MongoDB machine unavailable from batch data: {e}")
        return pd.DataFrame()
    
def get_subserie_unavailable_windows(db_manager, logger):
    try:
        with logger.context("Subserie Unavailability"):
            logger.info("Retrieving all dates when certain subseries are not available for production")
            today_dt = datetime.combine(datetime.today().date(), time.min)

            query_filter = {
                'subserieId': {'$exists': True, '$ne': None, '$ne': ''},
                'startDate': {'$exists': True, '$ne': None, '$ne': ''},
                'endDate': {'$exists': True, '$ne': None, '$ne': ''}
            }
            projection = {
                '_id': 0,
                'subserieId': 1,
                'startDate': 1,
                'endDate': 1
            }
            df = mongo_query(query_filter, projection, db_manager.subseries_unavailable, logger)
            
            past_end_date_mask = (df['endDate'].notna()) & (df['endDate'] < today_dt)
            past_end_date_count = past_end_date_mask.sum()
            if past_end_date_count > 0:
                logger.info(f"Identified {past_end_date_count} entries where endDate exists and is in the past. These will be discarded.")

            usable_mask = ~(past_end_date_mask)

            usable_df = df[usable_mask].copy()
            logger.addDataFrameRecords("Subserie unavailability entries to be used", usable_df)
            logger.info(f"Returning {len(usable_df)} usable restriction entries.")
            return usable_df
    except Exception as e:
        logger.error(f"Error fetching or processing subserie restriction data: {e}")
        return pd.DataFrame(), pd.DataFrame()
    
def mongo_query(query_filter, projection, collection, logger, write=True):
    with logger.context("Fetching, parsing and cleaning data"):
        try:   
            cursor = collection.find(query_filter, projection)
            return list_to_df(cursor, logger, write)
        except Exception as e:
            logger.error(f"Error querying MongoDB collection '{collection.name}': {e}", include_traceback=True)
            return pd.DataFrame()

def mongo_pipeline(pipeline, collection, logger):
    with logger.context("Fetching, parsing and cleaning data"):
        try:
            cursor = collection.aggregate(pipeline)
            return list_to_df(cursor, logger)
        except Exception as e:
            logger.error(f"Error querying MongoDB collection '{collection.name}': {e}", include_traceback=True)
            return pd.DataFrame()
        


    