import numpy as np
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from Application_logs.logger import App_logger


class Collect_Data_From_Any_Where:
    """
        This class shall be used for collect the data from Local, from any sql or Nosql
        Database, Any Cloud storage etc.
    """

    def __init__(self, format=None, LocalPath=None, DbPath=None, CloudPath=None):
        self.DataFormat = format
        self.LocalPath = LocalPath
        self.DbPath = DbPath
        self.CloudPath = CloudPath
        self.logger = App_logger()

    def get_formated_data_from_local(self):
        """
            Method Name: get_formated_data_from_local.
            Description: This method helps to get dataset from local storage in a particular format
                         (csv, tsv, xls, json, html, etc.
            Output:      Get the Dataset.
            On Failure:  Raise Error.

            Written By: Dibyendu Biswas
            Version: 1.0
            Revisions: None
        """
        try:
            for file in listdir(self.LocalPath):
                data = pd.read_csv("{path}/{file}".format(path=self.LocalPath, file=file))
                print(data)
                return data
        except Exception as e:
            print(e)
            raise e


if __name__ == "__main__":
    path = "../Raw Data/Titanic 2"
    format = "csv"
    data = Collect_Data_From_Any_Where(format=format, LocalPath=path)
    data.get_formated_data_from_local()
