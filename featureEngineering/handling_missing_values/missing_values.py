from Application_logs.logger import App_logger
from Collect_The_Data_FromAny_Where.collectData import Collect_Data_From_Any_Where as collect_data
import pandas as pd
import numpy as np
import os
class missingValues:
    def __init__(self, format, LocalPath=None, DbPath=None, CloudPath=None):
        self.data = collect_data(format, LocalPath, DbPath, CloudPath).get_formated_data_from_local()
        self.logger = App_logger()
    
    
if __name__ == "__main__":
