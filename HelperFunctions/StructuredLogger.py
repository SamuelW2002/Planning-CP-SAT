from datetime import datetime
import traceback
from typing import Dict, List, Optional
from pymongo import MongoClient, collection as MongoCollection
import pymongo

class HierarchicalLogger:
    ML_Logs: Optional[MongoCollection]
    User_Feedback: Optional[MongoCollection]

    def __init__(self, initial_root_name: str):
        self.db_manager = None
        self.feedback_list = []
        self._initialize_state(initial_root_name)

    def _initialize_state(self, root_name: str):
        self._log_data = {
            "name": root_name,
            "timestamp_start": datetime.now().isoformat(),
            "timestamp_end": None,
            "steps": {},
            "errors": {}
        }
        self._context_stack: List[Dict] = [self._log_data["steps"]]
        self._counter_stack: List[int] = [ 1 ]
        self._current_root_name = root_name

    def _get_current_context(self) -> Dict:
        return self._context_stack[-1]
    
    def _get_current_counter(self) -> int:
        return self._counter_stack[-1]
    
    def _increment_current_counter(self) -> None:
        self._counter_stack[-1] += 1

    def info(self, message: str):
        current_context = self._get_current_context()
        counter = self._get_current_counter()
        current_context["Log" + str(counter)] = message
        self._increment_current_counter()

    def error(self, message: str, include_traceback: bool = False):
        error_message = message
        if include_traceback:
            try:
                tb_str = traceback.format_exc()
                if tb_str and tb_str != "NoneType: None\n":
                    error_message += f"\nTraceback:\n{tb_str}"
            except Exception:
                pass
        current_context = self._get_current_context()
        counter = self._get_current_counter()
        current_context[str(counter)] = error_message
        self._increment_current_counter()

    def feedback(self, message: str):
        self.feedback_list.append(message)

    def get_feedback_list(self) -> List[str]:
        return self.feedback_list

    class _LogContext:
        def __init__(self, logger_instance, context_name: str):
            self.logger = logger_instance
            self.context_name = context_name

        def __enter__(self):
            parent_context = self.logger._get_current_context()
            safe_name = self.context_name.replace(".", "_").replace("$", "_")
            if safe_name not in parent_context:
                parent_context[safe_name] = {}
            self.logger._context_stack.append(parent_context[safe_name])
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type is not None:
                error_msg = (
                    f"Exception occurred in context '{self.context_name}': "
                    f"{exc_type.__name__}: {exc_val}"
                )
                self.logger.error(error_msg, include_traceback=True)
            self.logger._context_stack.pop()
            return False

    def context(self, name: str):
        if not name:
            safe_name = "UnnamedContext"
            self.error("Context name cannot be empty.")
        else:
            safe_name = name.replace(".", "_").replace("$", "_")
        return self._LogContext(self, safe_name)

    def set_collections(self, db_manager):
        self.ML_Logs = db_manager.ml_logs
        self.User_Feedback = db_manager.user_feedback
        self.info("Added ML_Log collection to the Logger instance")

    def addListRecords(self, name: str, records: List[Dict]):
        records_dict = { str(i+1): record for i, record in enumerate(records) }
        current_context = self._get_current_context()
        current_context[str(name)] = records_dict
        self._increment_current_counter()
    
    def addDataFrameRecords(self, name: str, df):
        records = df.to_dict(orient="records")
        records_dict = {str(i+1): record for i, record in enumerate(records)}
        current_context = self._get_current_context()
        current_context[str(name)] = records_dict
        self._increment_current_counter()

    def write_to_mongo(self):
        try:
            log_copy = self._log_data.copy()
            log_copy.pop('_id', None)
            print(log_copy)
            self.ML_Logs.insert_one(log_copy)
        except Exception as e:
            print(f"Error writing log to MongoDB: {e}")

    def write_feedback_to_mongo(self):
        with self.context("Writing Feedback"):
            self.info("Writing all user feedback logs to Mongo")
            feedback_list = self.get_feedback_list()
            self.addListRecords("Retrieved feedback list", feedback_list)

            if not isinstance(feedback_list, list):
                self.error(f"ERROR: Internal feedback data is not a list (type: {type(feedback_list)}).")
                return

            try:
                delete_result = self.User_Feedback.delete_many({})
                self.info(f"INFO: Deleted {delete_result.deleted_count} previous feedback document(s).")
                if feedback_list:
                    feedback_document = {f"{i+1}": message for i, message in enumerate(feedback_list)}
                    feedback_document["_createdAt"] = datetime.now()
                    insert_result = self.User_Feedback.insert_one(feedback_document)
                    self.info(f"Feedback document successfully written with ID: {insert_result.inserted_id}")
                else:
                    self.info("INFO: No feedback messages to insert.")
            except Exception as e:
                self.error(f"ERROR: Error writing feedback document to MongoDB: {e}")


    def finalize_and_reset(self, next_log_root_name: str):
        log_id = self.write_to_mongo()
        print("wrote to mongo and reset context")
        feedback_success = True
        try:
            self.write_feedback_to_mongo()
        except Exception as e:
            feedback_success = False

        self._initialize_state(next_log_root_name)

        return log_id, feedback_success


    def cleanup_old_logs(self):
        with self.context("Log Cleanup"):
            try:
                self.info(f"Starting cleanup for log collection, keeping latest 9 records.")

                cursor = self.ML_Logs.find(
                    {},
                    {"_id": 1}
                ).sort("_id", pymongo.DESCENDING).skip(9).limit(1)

                cutoff_docs = list(cursor)

                if not cutoff_docs:
                    self.info(f"Cleanup not needed: Collection has {self.ML_Logs.count_documents({})} records (<= 9).")
                    return

                cutoff_id = cutoff_docs[0]["_id"]
                self.info(f"Found cutoff _id: {cutoff_id}. Deleting records older than this.")

                delete_query = {"_id": {"$lt": cutoff_id}}
                result = self.ML_Logs.delete_many(delete_query)

                self.info(f"Cleanup successful: Deleted {result.deleted_count} older log records.")

            except Exception as e:
                self.error(f"Error during log cleanup for collection ml_logs: {e}", exc_info=True)

