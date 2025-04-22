import pandas as pd

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', 2000)        # Optional: Set a wide display width

from MongoDBFunctions.MongoQuerries import (
    get_all_cycle_data, get_all_technicians, get_all_orders,
    get_available_weekends,
    get_subserie_unavailable_windows
)
from PreCalculationFunctions.ProcessDataFunctions import (
    calculate_production_plan, calculate_unavailable_weekends,
    get_machine_unavailable_df
)
from PreCalculationFunctions.FilemakerFunctions import load_machine_names_df

class DataFrameHolder:
    available_weekends_df: pd.DataFrame
    technician_unavailability_df: pd.DataFrame
    machines_unavailable_df: pd.DataFrame
    all_orders_df: pd.DataFrame
    processed_data_df: pd.DataFrame
    subseries_unavailable_df: pd.DataFrame
    ongoing_past_issues_df: pd.DataFrame
    all_tasks_df: pd.DataFrame
    machine_names_df: pd.DataFrame
    weekends_unavailable_df: pd.DataFrame

    preparation_intervals_df: pd.DataFrame
    scheduled_orders_df: pd.DataFrame

    def __init__(self, db_manager, logger):
        self.db_manager = db_manager
        self.logger = logger
        self._fetch_and_process_all()

    def _fetch_and_process_all(self):
        with self.logger.context("Retrieving MongoDB Data"):
            self.available_weekends_df = get_available_weekends(self.db_manager, self.logger)
            self.technician_unavailability_df = get_all_technicians(self.db_manager, self.logger)
            self.machines_unavailable_df = get_machine_unavailable_df(self.db_manager, self.logger)
            self.all_orders_df = get_all_orders(self.db_manager, self.logger)
            self.processed_data_df = get_all_cycle_data(self.db_manager, self.logger)

            unavailable_subseries_df = get_subserie_unavailable_windows(self.db_manager, self.logger)
            self.subseries_unavailable_df = unavailable_subseries_df

            self.machine_names_df = load_machine_names_df(self.logger)

            with self.logger.context("InitialDataProcessing"):
                self.all_tasks_df = calculate_production_plan(self, self.logger)
                self.weekends_unavailable_df = calculate_unavailable_weekends(self, self.logger)
            self.logger.info("Finished fetching and processing all input data.")