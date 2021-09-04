import numpy as np
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from Application_logs.logger import App_logger
from Collect_The_Data_FromAny_Where.collectData import Collect_Data_From_Any_Where as collect_data


class Training_Raw_Data_Validation:
    """
        This class shall be used for handling all the Raw Data validations:

        Written By: Dibyendu Biswas
        Version: 1.0
        Revisions: None
    """

    def __init__(self, format, LocalPath=None, DbPath=None, CloudPath=None):
        self.data = collect_data(format, LocalPath, DbPath, CloudPath).get_formated_data_from_local()
        self.logger = App_logger()

    def create_Directory_For_Good_Bad_Raw_Data(self):
        """
           Method Name: create_Directory_For_Good_Bad_Raw_Data
           Description: This method creates directories to store the Good Data and Bad Data
                        after trining data.

           Output: Create the directory Good_Raw and Bad_Raw in Training_Raw_files_validated.
           On Failure: OSError

           Written By: Dibyendu Biswas
           Version: 1.0
           Revisions: None
           """

        try:
            file = open("../Execution Logs/Training/General_Logs.txt", 'a+')
            path = os.path.join("../Training_Raw_files_validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
                self.logger.log(file, "Successfully created Good_Raw directory\n")
            path = os.path.join("../Training_Raw_files_validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
                self.logger.log(file, "Successfully created Bad_Raw directory\n")
            file.close()

        except OSError as e:
            file = open("../Execution Logs/Training/General_Logs.txt", 'a+')
            self.logger.log(file, "Error while creating the Directory %s: " % e + "\n")
            file.close()
            raise OSError

    def delete_Existing_Good_Raw_Data_Training_Folder(self):
        """
            Method Name: delete_Existing_Good_Raw_Data_Training_Folder
            Description: This method deletes the directory made to store the Good Data
                         after loading the data in the table.

            Output: Delete the Good_Raw directory from Training_Raw_files_validated folder.
            On Failure: OSError

            Written By: Dibyendu Biswas.
            Version: 1.0
            Revisions: None
        """
        try:
            path = "../Training_Raw_files_validated/"
            if os.path.isdir(path + "Good_Raw/"):
                shutil.rmtree(path + "Good_Raw/")
                file = open("../Execution Logs/Training/General_Logs.txt", 'a+')
                self.logger.log(file, "Good_Raw directory deleted before starting validation!!! \n")
                file.close()

        except OSError as e:
            file = open("../Execution Logs/Training/General_Logs.txt", 'a+')
            self.logger.log(file, "Error while delete the Good_Raw directory %s: " % e + "\n")
            file.close()
            raise OSError

    def delete_Existing_Bad_Raw_Data_Training_Folder(self):
        """
            Method Name: delete_Existing_Bad_Raw_Data_Training_Folder
            Description: This method deletes the directory made to store the Bad_Raw Data
                         after loading the data in the table.

            Output: Delete the Bad_Raw directory from Training_Raw_files_validated folder.
            On Failure: OSError

            Written By: Dibyendu Biswas.
            Version: 1.0
            Revisions: None
        """
        try:
            path = "../Training_Raw_files_validated/"
            if os.path.isdir(path + "Bad_Raw/"):
                shutil.rmtree(path + "Bad_Raw/")
                file = open("../Execution Logs/Training/General_Logs.txt", 'a+')
                self.logger.log(file, "Bad_Raw directory deleted before starting validation!!!\n")
                file.close()

        except OSError as e:
            file = open("../Execution Logs/Training/General_Logs.txt", 'a+')
            self.logger.log(file, "Error while delete the Bad_Raw directory %s: " % e + '\n')
            file.close()
            raise OSError

    def manual_Regex_Creation(self):
        """
            Method Name: manual_Regex_Creation.
            Description: This method contains a manually defined regex based on the Given
                         dataset. This Regex is used to validate the data for training.
            On Failure:  None.

            Written By: Dibyendu Biswas
            Version: 1.0
            Revisions: None

        """
        regex = "['\_'']+[\d_]+[\d]+\.csv+\.tsv"
        return regex

    def get_numerical_features(self):
        """
            Method Name: get_numerical_features.
            Description: This method helps to get numerical features from the given data.

            Output: Get list of numerical features.
            On Failure: Raise Error.

            Written By: Dibyendu Biswas
            Version: 1.0
            Revisions: None
        """
        try:
            file = open("../Execution Logs/Training/Raw_Validation_Logs.txt", 'a+')
            dataFrame = self.data
            numerical_feature = []
            for col in dataFrame.columns:
                if dataFrame[col].dtypes in ['int64', 'float64', 'int32', 'float32']:
                    numerical_feature.append(col)
            self.logger.log(file, "Get the numerical features from given dataset {cols}\n".format(cols=numerical_feature))
            file.close()
            return numerical_feature

        except Exception as e:
            file = open("../Execution Logs/Training/Raw_Validation_Logs.txt", 'a+')
            self.logger.log(file, "Error while get the numerical features %s: " % e + '\n')
            file.close()
            raise e

    def get_categorical_features(self):
        """
            Method Name: get_categorical_features.
            Description: This method helps to get categorical features from the given data

            Output: Get list of categorical features.
            On Failure: Raise Error.

            Written By: Dibyendu Biswas
            Version: 1.0
            Revisions: None
        """
        try:
            file = open("../Execution Logs/Training/Raw_Validation_Logs.txt", 'a+')
            dataFrame = self.data
            categorical_feature = []
            for col in dataFrame.columns:
                if dataFrame[col].dtypes not in ['int64', 'float64', 'int32', 'float32']:
                    categorical_feature.append(col)
            self.logger.log(file, "Get the categorical features from given dataset {cols}\n".format(cols=categorical_feature))
            file.close()
            return categorical_feature

        except Exception as e:
            file = open("../Execution Logs/Training/Raw_Validation_Logs.txt", 'a+')
            self.logger.log(file, "Error while get the categorical features %s: " % e + '\n')
            file.close()
            raise e

    def get_length_of_row_col(self):
        """
            Method Name: get_length_of_row_col
            Description: Get the Lengthof Row and Column.

            Output: length of row and column.
            On Failure: Raise Error.

            Written By: Dibyendu Biswas
            Version: 1.0
            Revisions: None
        """
        try:
            file = open("../Execution Logs/Training/Raw_Validation_Logs.txt", 'a+')
            dataFrame = self.data
            len_row, len_col = dataFrame.shape
            self.logger.log(file, "Get the length of row: {row} and column: {col}\n".format(row=len_row, col=len_col))
            file.close()
            return len_row, len_col

        except Exception as e:
            file = open("../Execution Logs/Training/Raw_Validation_Logs.txt", 'a+')
            self.logger.log(file, "Error while get the length of row and column %s: " % e + '\n')
            file.close()
            raise e

    def check_missing_value_present(self):
        """
            Method Name: check_missing_value_present
            Description: This function helps to check if there are few missing value present or not in
                         all coulumns

            Output: If missing value present then show true, otherwise false.
            On Failure:  Exception.

            Written By: Dibyendu Biswas
            Version: 1.0
            Revisions: None
        """
        try:
            dataFrame = self.data
            count = 0
            for missing_val in dataFrame.isnull().sum():
                if missing_val != 0:
                    count += 1
                    break

            if count == 0:
                file = open("../Execution Logs/Training/Raw_Validation_Logs.txt", 'a+')
                self.logger.log(file, "Missing value is not present in these dataset\n")
                file.close()
                return False
            else:
                file = open("../Execution Logs/Training/Raw_Validation_Logs.txt", 'a+')
                self.logger.log(file, "Missing value is present in these dataset\n")
                file.close()
                return True

        except Exception as e:
            file = open("../Execution Logs/Training/Raw_Validation_Logs.txt", 'a+')
            self.logger.log(file, "Error while check the missing value present or not in dataset %s: " % e + '\n')
            file.close()
            print(e)
            raise e

    def check_data_balanced_or_not(self, y):
        """
            Method Name: check_data_balanced_or_not
            Description: This function helps to check the dataset is balanced or imbalanced.

            Output: Balanced (True) or Imbalanced (False) (with ratio)
            On Failure: Exception.

            Written By: Dibyendu Biswas
            Version: 1.0
            Revisions: None
        """
        try:
            file = open("../Execution Logs/Training/Raw_Validation_Logs.txt", 'a+')
            dataFrame = self.data
            if dataFrame[y].value_counts()[0] == dataFrame[y].value_counts()[1]:
                self.logger.log(file, "The given dataset is balanced\n")
                file.close()
                return True
            else:
                self.logger.log(file, "The given dataset is not balanced {ratio}\n".format(ratio=dict(dataFrame[y].value_counts())))
                file.close()
                return False

        except Exception as e:
            file = open("../Execution Logs/Training/Raw_Validation_Logs.txt", 'a+')
            self.logger.log(file, "Error while check the dataset is balanced or imbalanced %s: " % e + '\n')
            file.close()
            print(e)
            raise e

    def detect_the_outliers(self):
        """
            Method Name: detect_the_outliers.
            Description: This function helps to check if there are any outliers present or not
                         using IQR technique.

            Output: If Outliers is present then pass columns.
            On Failure: Exception.

            Written By: Dibyendu Biswas
            Version: 1.0
            Revisions: None
        """
        try:
            file = open("../Execution Logs/Training/Raw_Validation_Logs.txt", 'a+')
            numerical_features = self.get_numerical_features()
            q1 = {}
            q3 = {}
            iqr = {}
            for col in self.data[numerical_features]:
                q11, q33 = np.percentile(self.data[col], [25, 75])
                q1[col] = q11
                q3[col] = q33
            for key1, val1 in q3.items():
                for key2, val2 in q1.items():
                    if key1 == key2:
                        iqr[key1] = val1 - val2
            lb = {}
            ub = {}
            for key1, val1 in q1.items():
                for key2, val2 in iqr.items():
                    if key1 == key2:
                        lb[key1] = val1 - 1.5 * val2

            for key1, val1 in q3.items():
                for key2, val2 in iqr.items():
                    if key1 == key2:
                        ub[key1] = val1 + 1.5 * val2
            get_cols = []
            for col in self.data[numerical_features]:
                for key1, val1 in lb.items():
                    for key2, val2 in ub.items():
                        if col == key1 == key2:
                            for i in self.data[col]:
                                if i < val1 or i > val2:
                                    break
                            if col not in get_cols:
                                get_cols.append(col)
            self.logger.log(file, "Get those columns where outliers are present {cols}\n".format(cols=get_cols))
            file.close()
            return get_cols

        except Exception as e:
            file = open("../Execution Logs/Training/Raw_Validation_Logs.txt", 'a+')
            self.logger.log(file, "Error while detect the outliers in dataset %s: " % e + '\n')
            file.close()
            print(e)
            raise e

    def detect_null_values(self):
        """
            Method Name: detect_null_values.
            Description: This function helps to detect the null values means where and in which
                         columns null values are present.

            Output: Return the particular columns and index value (where null value is present).
            On Failure: Exception

            Written By: Dibyendu Biswas
            Version: 1.0
            Revisions: None
        """
        try:
            file = open("../Execution Logs/Training/Raw_Validation_Logs.txt", 'a+')
            dataFrame = self.data
            dict = {}
            count = 0
            for cols in dataFrame:
                if dataFrame[cols].isnull().sum() > 0:
                    dict[cols] = np.where(dataFrame[cols].isnull() == True)
                    count += 1

            if count == 1:
                self.logger.log(file, "Get those columns and index values where null value is present {dict}\n".format(dict=dict))
                file.close()
                return dict
            else:
                self.logger.log(file, "Null values are not present in given dataset\n")
                file.close()

        except Exception as e:
            file = open("../Execution Logs/Training/Raw_Validation_Logs.txt", 'a+')
            self.logger.log(file, "Error while detect the null values in dataset %s: " % e + '\n')
            file.close()
            print(e)
            raise e

    def remove_duplicate_values(self):
        """
            Method Name: remove_duplicate_values.
            Descriptions: This function helps to remove the duplicates values from the
                          given datasets.

            Output: remove the duplicate values
            On Failure: Raise Exception.

            Written By: Dibyendu Biswas
            Version: 1.0
            Revisions: None
        """
        try:
            file = open("../Execution Logs/Training/Raw_Validation_Logs.txt", 'a+')
            dataFrame = self.data
            dataFrame.drop_duplicates(keep=False)
            self.logger.log(file, "Get the data after removing the duplicate values\n")
            file.close()
            return dataFrame

        except Exception as e:
            file = open("../Execution Logs/Training/Raw_Validation_Logs.txt", 'a+')
            self.logger.log(file, "Error while removing the duplicate values in dataset %s: " % e + '\n')
            file.close()
            print(e)
            raise e

    def validate_the_whole_missing_values(self, columns=None):
        """
            Method Name: validate_the_whole_missing_values
            Description: For batch files, if all the values are missing in a particular colum or columns,
                         then the file is not suitable for processing.
                         Such files are moved to Bad Data Folder and after that delete those data.

            Output: If True means Stop the pipeline (dataset is not suitable for training)
                    else (False) go to next step.
            On Failure: Exception.

            Written By: Dibyendu Biswas
            Version: 1.0
            Revisions: None
        """
        try:
            file = open("../Execution Logs/Training/Raw_Validation_Logs.txt", 'a+')
            dataFrame = self.data
            count = 0
            for cols in dataFrame:
                if (len(dataFrame[cols]) - dataFrame[cols].count()) == len(dataFrame[cols]):
                    count += 1
                    break
            if count == 1:
                self.logger.log(file, "All values are missing in given dataset\n")
                file.close()
                return True
            else:
                self.logger.log(file, "All values are not missing in given dataset\n")
                file.close()
                return False

        except OSError as e:
            file = open("../Execution Logs/Training/Raw_Validation_Logs.txt", 'a+')
            self.logger.log(file, "Error while validate all missing values in dataset %s: " % e + '\n')
            file.close()
            print(OSError)
            raise OSError


if __name__ == "__main__":
    path = "../Raw Data/iris Data ".strip()
    format = "csv"
    data = Training_Raw_Data_Validation(format=format, LocalPath=path)
    data.create_Directory_For_Good_Bad_Raw_Data()
    data.delete_Existing_Good_Raw_Data_Training_Folder()
    data.delete_Existing_Bad_Raw_Data_Training_Folder()

    data.get_numerical_features()
    data.get_categorical_features()
    data.get_length_of_row_col()
    data.check_missing_value_present()
    data.check_data_balanced_or_not(y="species")
    data.detect_the_outliers()
    data.detect_null_values()
    data.remove_duplicate_values()
    data.validate_the_whole_missing_values()
