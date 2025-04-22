import traceback
from MongoDBFunctions.MongoManagerClass import DatabaseManager
from MongoDBFunctions.MongoWriterFunctions import write_scheduled_orders_to_mongo
from CPSAT.CalculateSchedule import create_optimized_schedule_df
from PreCalculationFunctions.AuthenticationFunctions import authorize_filemaker_data_api, logout_filemaker_data_api
from HelperFunctions.StructuredLogger import HierarchicalLogger 
from Domain.DataFramesClass import DataFrameHolder

def calculate_planning(duration):
    logger = HierarchicalLogger("Preparation Logs")
    try:
        db_manager = DatabaseManager(logger=logger)
        if not db_manager.client:
            logger.error("DatabaseManager failed to connect during initialization.")
            raise ConnectionError("Database Manager failed to connect.")

        authorize_filemaker_data_api(logger)

        dataframes = DataFrameHolder(db_manager, logger)

        logger.finalize_and_reset("CPSAT Log")
        create_optimized_schedule_df(dataframes, logger, duration)
        write_scheduled_orders_to_mongo(db_manager, logger, dataframes)
        logger.write_feedback_to_mongo()

    except Exception as e:
        traceback.print_exc()
        if logger:
            logger.error(f"Unhandled exception in calculate_planning: {type(e).__name__} - {e}", include_traceback=True)
    finally:
        with logger.context("Finalization"):
            try:
                 logger.info("Logging out from FileMaker API...")
                 logout_filemaker_data_api()
                 logger.info("FileMaker logout successful.")
            except Exception as fm_e:
                 logger.error(f"Filemaker logout failed: {fm_e}")

            logger.cleanup_old_logs()
            logger.info("Writing main hierarchical log to MongoDB...")
            print("writing to mongo")
            logger.write_to_mongo()
            if db_manager:
                logger.info("Closing DB connection...")
                db_manager.close_connection()
        print("Finished calculate_planning function execution.")


def calculate_order_date():
    return