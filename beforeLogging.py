import os
import logging
import time
def createLogPath():
    logPath = os.path.join('log\\', time.strftime("%Y%m"))
    try:
        os.makedirs(logPath)
    except:
        logging.writeLog("Log folder existed")
        