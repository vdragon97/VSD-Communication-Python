import time
import os
def writeLog(logMsg):  
    logPath = 'log/' + time.strftime("%Y%m")
    fileName = time.strftime("%Y%m%d") + ".log"
    logTime = time.strftime("%Y-%m-%d %H:%M:%S")
    logFile = open(os.path.join(logPath, fileName), "a+")
    print("[" + logTime + "]---[" + logMsg + "]")
    logFile.write("[" + logTime + "]---[" + logMsg + "]\n")
    logFile.close()