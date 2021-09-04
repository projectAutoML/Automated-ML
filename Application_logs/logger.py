from datetime import datetime
import logging
"""
    This class or package is help to logs each and every
    informations.
"""
class App_logger:
    def __init__(self):
        pass

    def log(self, file_object, log_message):
        self.now = datetime.now()
        self.date = self.now.date()
        self.current_time = self.now.strftime("%H:%M:%S")
        file_object.write(
            str(self.date) + "/" + str(self.current_time) + "\t\t" + log_message)
def getLog(self,nm):
        # Creating custom logger
    logger = logging.getLogger(nm)
        # reading contents from properties file
    f = open("properties.txt", 'r')
    if f.mode == "r":
        loglevel = f.read()
    if loglevel == "ERROR":
        logger.setLevel(logging.ERROR)
    elif loglevel == "DEBUG":
        logger.setLevel(logging.DEBUG)
        # Creating Formatters
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')
        # Creating Handlers
    file_handler = logging.FileHandler('test.log')
        # Adding Formatters to Handlers
    file_handler.setFormatter(formatter)
        # Adding Handlers to logger
    logger.addHandler(file_handler)
    return logger

    
