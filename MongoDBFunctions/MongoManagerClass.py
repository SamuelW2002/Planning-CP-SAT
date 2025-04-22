from pymongo import MongoClient, collection as MongoCollection
from pymongo.database import Database as MongoDatabase
from HelperFunctions.EnvironmentVariableLoader import get_mongodb_uri_from_env
from HelperFunctions.StructuredLogger import HierarchicalLogger
import os
from typing import Dict, Optional

MACHINE_LEARNING_DB_NAME = "Machine_Learning"
PLANNING_DB_NAME = "planning"
LOGGING_DB_NAME = "ML_Calculation_Logs"

ML_COLLECTIONS = {
    "avg_cycle_times": "ML_Avg_Cycle_Times",
    "open_order_schedule": "Open_Order_Machine_Schedule",
    "possible_tasks": "Possible_Tasks_For_Open_Orders",
    "user_feedback": "user_feedback"
}

PLANNING_COLLECTIONS = {
    "batches_to_plan": "batches",
    "ai_planning_suggestion": "AIPlanning",
    "resource_time_ranges": "resourcetimeranges",
    "machines_unavailable": "onderhoud",
    "subseries_unavailable": "matrijsStatus",
}

LOGGING_COLLECTIONS = {
    "error_logs": "Error_Logs",
    "open_orders_logs": "Open_Orders_Logs",
    "ml_logs": "ML_Logs",
    "processed_orders_logs": "Processed_Orders_Logs",
}


class DatabaseManager:
    client: Optional[MongoClient]
    ml_db: Optional[MongoDatabase]
    planning_db: Optional[MongoDatabase]
    logging_db: Optional[MongoDatabase]
    avg_cycle_times: Optional[MongoCollection]
    open_order_schedule: Optional[MongoCollection]
    possible_tasks: Optional[MongoCollection]
    user_feedback: Optional[MongoCollection]
    batches_to_plan: Optional[MongoCollection]
    ai_planning_suggestion: Optional[MongoCollection]
    resource_time_ranges: Optional[MongoCollection]
    machines_unavailable: Optional[MongoCollection]
    subseries_unavailable: Optional[MongoCollection]
    error_logs: Optional[MongoCollection]
    open_orders_logs: Optional[MongoCollection]
    ml_logs: Optional[MongoCollection]
    processed_orders_logs: Optional[MongoCollection]


    def __init__(self, logger: Optional[HierarchicalLogger]):
        with logger.context("DatabaseInitialization"):
            logger.info("Attempting DatabaseManager initialization...")
            self.logger = logger
            self.logger.context("Initializing DatabaseManager")
            self.mongo_uri = get_mongodb_uri_from_env()
            self.client = None
            self.ml_db = None
            self.planning_db = None
            self.logging_db = None
            self._connect_and_setup_collections()
            logger.info("DatabaseManager initialization complete.")
            logger.info("Passing required collections to logger instance")
            logger.set_collections(self)

    def _connect_and_setup_collections(self):
        try:
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            self.logger.info(f"Successfully connected to MongoDB instance.")

            self.ml_db = self.client[MACHINE_LEARNING_DB_NAME]
            self.planning_db = self.client[PLANNING_DB_NAME]
            self.logging_db = self.client[LOGGING_DB_NAME]

            self.logger.info(f"Setting up collections for DB: {MACHINE_LEARNING_DB_NAME}")
            self._create_collection_attributes(self.ml_db, ML_COLLECTIONS)
            self.logger.info(f"Setting up collections for DB: {PLANNING_DB_NAME}")
            self._create_collection_attributes(self.planning_db, PLANNING_COLLECTIONS)
            self.logger.info(f"Setting up collections for DB: {LOGGING_DB_NAME}")
            self._create_collection_attributes(self.logging_db, LOGGING_COLLECTIONS)

        except Exception as e:
            self.logger.error(f"CRITICAL: Error connecting to MongoDB or setting up collections: {e}")
            self.client = None; self.ml_db = None; self.planning_db = None; self.logging_db = None
            raise ConnectionError(f"Failed to connect to MongoDB or setup collections: {e}")

    def _create_collection_attributes(self, db_handle: Optional[MongoDatabase], collection_map: Dict[str, str]):
        for attr_name, coll_name in collection_map.items():
            try:
                collection_obj = db_handle[coll_name]
                setattr(self, attr_name, collection_obj)
                self.logger.info(f"   - Collection '{coll_name}' accessible as db_manager.{attr_name}")
            except Exception as e:
                 self.logger.error(f"   - Failed to access collection '{coll_name}' for attribute '{attr_name}': {e}")
                 setattr(self, attr_name, None)

    def close_connection(self):
        if self.client:
            try:
                self.client.close()
                self.logger.info("MongoDB connection closed.")
            except Exception as e:
                 self.logger.error(f"Error closing MongoDB connection: {e}")
        self.client = None