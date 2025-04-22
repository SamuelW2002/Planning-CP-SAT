from dataclasses import dataclass, field
from datetime import datetime
import pandas as pd
import uuid
from typing import List, Dict, Any

@dataclass
class PossibleTask:
    order_row: pd.Series = field(repr=False)
    machine_row: pd.Series = field(repr=False)
    orderID_str: str

    subserieID: int = field(init=False)
    machineID: int = field(init=False)
    articleID: Any = field(init=False)
    dueDate: datetime = field(init=False)
    iml_requested: bool = field(init=False)
    amount_order: int = field(init=False)
    article_description: str = field(init=False)
    priority_code_raw: int = field(init=False)
    info_messages: List[Any] = field(default_factory=list)
    mongo_id: str = field(init=False)

    iml_possible_machine: bool = field(init=False)
    cav: int = field(init=False)
    cycle_avg: float = field(init=False)
    machine_status: int = field(init=False)
    machine_maintenance_windows: List[Dict[str, Any]] = field(init=False)

    secondsNeeded: int = field(init=False)
    priority_code: int = field(init=False) # Adjusted priority
    default_cav_used: bool = field(init=False, default=False)
    default_cycle_avg_used: bool = field(init=False, default=False)

    description: str = field(init=False)
    hotrunner: str = field(init=False)
    matrijsName: str = field(init=False)

    def __post_init__(self):
        self.description = str(self.order_row["description"])
        self.hotrunner = str(self.order_row["hotrunner"]) if pd.notna(self.order_row['hotrunner']) else "No Hotrunner Needed"
        self.matrijsName = str(self.order_row["matrijsName"])

        self.subserieID = int(self.order_row["subserieID"])
        self.dueDate = self.order_row["leverDatum"]
        self.iml_requested = bool(self.order_row['iml'])
        self.amount_order = int(self.order_row["aantal"])
        self.priority_code_raw = int(self.order_row["priority"]) if pd.notna(self.order_row["priority"]) else 0
        self.mongo_id = str(self.order_row["id"])

        self.machineID = self.machine_row['machineID']
        self.iml_possible_machine = bool(self.machine_row['IML'])
        self.cav = int(self.machine_row['cav']) if pd.notna(self.machine_row['cav']) else 0
        self.cycle_avg = float(self.machine_row['cycleAvg']) if pd.notna(self.machine_row['cycleAvg']) else 10.0 # Default if missing

        # If no cycle time in data, cycle time is 10
        if self.cycle_avg == 10.0:
             self.default_cycle_avg_used = True

        # If no cavity amount in data, use 4
        if self.cav == 0:
            self.cav = 4
            self.default_cav_used = True

        cycles_needed = self.amount_order / self.cav
        self.secondsNeeded = round(cycles_needed * self.cycle_avg)

        # Priority code is default 5
        self.priority_code = self.priority_code_raw if self.priority_code_raw != 0 else 6

        # Set defaults for status/maintenance (can be refined later)
        self.machine_status = 0 # int(self.machine_row.get('machine_status', 0))
        self.machine_maintenance_windows = [] # self.machine_row.get('machine_maintenance_windows', [])


    def to_dict(self) -> Dict[str, Any]:
        return {
            'mongoID': self.mongo_id,
            'orderID': self.orderID_str,
            'subserieID': self.subserieID,
            'machineID': self.machineID,
            'secondsNeeded': self.secondsNeeded,
            'dueDate': self.dueDate,
            'IML possible': self.iml_possible_machine,
            'priority_code': self.priority_code,
            'machine_status': self.machine_status,
            'machine_maintenance_windows': self.machine_maintenance_windows,
            'info_messages': self.info_messages,
            'description': self.description,
            'hotrunner': self.hotrunner,
            'matrijsName': self.matrijsName
        }