from datetime import datetime
import re
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

def DTS(target_time, logger):
    if pd.isna(target_time):
        logger.error(f"Input time is invalid (NaN/None/NaT): {target_time}.")
        return None
    try:
        if isinstance(target_time, pd.Timestamp):
            target_time_dt = target_time.to_pydatetime()
        elif isinstance(target_time, datetime):
            target_time_dt = target_time
        else:
             logger.error(f"Input time '{target_time}' is not a recognized datetime type (datetime or pd.Timestamp).")
        total_seconds = int((target_time_dt - datetime.now()).total_seconds())
        return total_seconds
    except TypeError as te:
        logger.error(f"Error converting time '{target_time}' due to timezone mismatch or comparison issue: {te}")
    except Exception as e:
        logger.error(f"Unexpected error converting time '{target_time}' to seconds: {e}")
        return None

def parseDates(df: pd.DataFrame, logger):
    logger.info("Attempting to parse 'startDate' and 'endDate' columns...")
    initial_rows = len(df)
    target_cols = ['startDate', 'endDate']

    processed_date_cols = []

    for col in target_cols:
        if col in df.columns:
            if not is_datetime64_any_dtype(df[col]):
                logger.info(f"  Parsing column '{col}' (current dtype: {df[col].dtype})...")
                df[col] = pd.to_datetime(df[col], errors='coerce')
                if is_datetime64_any_dtype(df[col]):
                    processed_date_cols.append(col)
                else:
                    logger.info(f"Column '{col}' could not be converted to datetime type after parsing")
            else:
                 logger.info(f"Column '{col}' is already datetime type")
                 processed_date_cols.append(col) # Add existing datetime cols too
        else:
            logger.info(f"Column '{col}' not found in DataFrame, skipping")

    if processed_date_cols:
        logger.info(f"Removing rows where NaT exists in processed date columns: {processed_date_cols}...")
        df = df.dropna(subset=processed_date_cols)
    else:
        logger.info("No target date columns found or processed; skipping NaT row removal")

    final_rows = len(df)
    rows_dropped = initial_rows - final_rows

    if processed_date_cols:
        if rows_dropped > 0:
            logger.info(f"Removed {rows_dropped} rows due to NaT in date columns. Final rows: {final_rows}")
        else:
            logger.info("No rows removed (either no NaT found or no relevant columns existed)")

    if df.empty and initial_rows > 0:
        logger.info("DataFrame is empty after removing rows with failed date parsing")

    return df

def checkMissingColumns(df, logger):
    logger.info("Checking for missing values in essential rows")
    initial_rows = len(df)
    id_keywords = ["subserieid", "machineid", "resourceid"]
    id_columns_to_check = [
        col for col in df.columns if any(keyword in col.lower() for keyword in id_keywords)
    ]

    if not id_columns_to_check:
        logger.info(f"No columns matching ID keywords found. Skipping missing ID check. Keywords: {id_keywords}")
    else:
        logger.info(f"Found potential ID columns to check for missing values: {id_columns_to_check}")

        df_original = df.copy()

        df.dropna(subset=id_columns_to_check, inplace=True)

        dropped_indices = df_original.index.difference(df.index)

        dropped_df = df_original.loc[dropped_indices]

        final_rows = len(df)
        rows_dropped = initial_rows - final_rows

        if rows_dropped > 0:
            logger.addDataFrameRecords(f"Dropped {rows_dropped} rows due to missing values in ID columns", dropped_df)
            if final_rows == 0:
                logger.info("DataFrame is now empty after dropping rows with missing IDs")
                return pd.DataFrame()
        else:
            logger.info(f"No rows needed dropping based on missing values in ID columns ({id_columns_to_check})")

    return df

def list_to_df(cursor, logger, write):
    list_cursor = list(cursor)
    if not list_cursor:
        logger.info(f"No documents found in MongoDB collection matching filter")
        return pd.DataFrame()
    if write:
        logger.info("Retrieved all data from MongoDB")

    try:
        df = pd.DataFrame(list_cursor)
        if df.empty:
            logger.info("MongoDB query returned data, but resulted in an empty DataFrame")
            return pd.DataFrame()
        if write:
            logger.info("Converted retrieved documents into DataFrame")

        df = checkMissingColumns(df, logger)
        df = parseDates(df, logger)
        return df
    except Exception as e:
        logger.error(f"Error converting MongoDB results to DataFrame: {e}", include_traceback=True)
        return pd.DataFrame()
