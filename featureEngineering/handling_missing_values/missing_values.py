from Application_logs.logger import App_logger
from Collect_The_Data_FromAny_Where.collectData import Collect_Data_From_Any_Where as collect_data
import pandas as pd
import numpy as np
import os
class missingValues:
    def __init__(self, format, LocalPath=None, DbPath=None, CloudPath=None):
        self.data = collect_data(format, LocalPath, DbPath, CloudPath).get_formated_data_from_local()
        self.logger = App_logger()
    
    
    def find_mean(self,mean_meadian_mode,col_name):
        """
        This class is used for replacing the missing values of a column of the data with mean:

        Written By: Abhilash Chaparala
        Version: 1.0
        Revisions: None
        """
        
        try:
            
            if mean_meadian_mode == 'mean':
                self.data[col_name].fillna(self.data.col_name.mean())
                self.logger.info('column {}\'s missing values are replaced with {}'.format(col_name,mean_meadian_mode))
                return True
        except:
            
            self.logger.error('column {}\'s missing values are not replaced with {}'.format(col_name,mean_meadian_mode))
            return False
        
    
    
    def find_median(self,mean_meadian_mode,col_name):
        """
        This class is used for replacing the missing values of a column of the data with median:

        Written By: Abhilash Chaparala
        Version: 1.0
        Revisions: None
        """
        try:
            
            if mean_meadian_mode == 'median':
                
                self.data[col_name].fillna(self.data.col_name.median())
                self.logger.info('column {}\'s missing values are replaced with {}'.format(col_name,mean_meadian_mode))
                return True
        
        except:
            self.logger.error('column {}\'s missing values are not replaced with {}'.format(col_name,mean_meadian_mode))
            return False   
    
    
    def find_mode(self,mean_meadian_mode,col_name):
        """
        This class is used for replacing the missing values of a column of the data with mode:

        Written By: Abhilash Chaparala
        Version: 1.0
        Revisions: None
        """
        try:
            
            if mean_meadian_mode == 'mode':
                self.data[col_name].fillna(self.data.col_name.mode())
                self.logger.info('column {}\'s missing values are replaced with {}'.format(col_name,mean_meadian_mode))
                return True
        
        except:
            self.logger.error('column {}\'s missing values are not replaced with {}'.format(col_name,mean_meadian_mode))
            return False
        
        


if __name__ == "__main__":
    pass
