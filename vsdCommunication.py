import platform
import sys
import oracleConnect
import cx_Oracle
import menu
import time
import beforeLogging
import logging
import json
import signal
import os
#from pathlib import Path
import sndeq

def __main__(workFlow, market):
    currentPlatform = platform.system()
    beforeLogging.createLogPath()
    logging.writeLog("Our current system platform: " + currentPlatform)
    os.environ["NLS_LANG"] = ".AL32UTF8"                
    myConfigDict = json.load(open('./config', 'r')) #load JSON file config
    mySndEqDict = json.load(open('./sndeq', 'r'))
    while True:
        logging.writeLog("Question: Do you want to use default config? (Y/N answer) (Y for continue without editing):")
        defaultYN = input()
        logging.writeLog("Answer: " + defaultYN)
        if defaultYN.upper() in ("Y","N"):
            break
    if defaultYN.upper() in ("N"):
        while True:
            logging.writeLog("Which key do you want to edit? (Type done to finish config)")
            for dictKey in myConfigDict.keys():
                logging.writeLog(dictKey)
            keyForEdit = input()
            logging.writeLog("Key for edit: " + keyForEdit)
            if keyForEdit == "done":
                break
            while True:
                if keyForEdit not in myConfigDict.keys():
                    break
                if keyForEdit in myConfigDict.keys():
                    logging.writeLog("Input your new value:")
                    valueForEdit = input()
                    logging.writeLog("Value for edit: " + valueForEdit)
                    newDict = {keyForEdit: valueForEdit}
                    myConfigDict.update(newDict)
                    with open('./config','w') as updateFileConfig: #edit here
                        json.dump(myConfigDict, updateFileConfig)
                    break
    #Load config file
    USER = myConfigDict.get('user')
    PASSWORD = myConfigDict.get('password')
    PORT = myConfigDict.get('port')
    SERVICENAME = myConfigDict.get('serviceName')
    HOST = myConfigDict.get('host')
    TABLESESSION = myConfigDict.get('tableSession')
    SLEEPTIME = myConfigDict.get('sleepTime')
    __VSDSNDEQ__ = myConfigDict.get('tableVsdSendEq')
    __VSDRCVEQ__ = myConfigDict.get('tableVsdRecvEq')
    __VSDCSVEQ__ = myConfigDict.get('tableVsdCsvEq')
    __VSDSNDDR__ = myConfigDict.get('tableVsdSendDr')
    __VSDRCVDR__ = myConfigDict.get('tableVsdRecvDr')
    logging.writeLog("Load common config file successfully")
    #Load sndeq config file
    localSndEq = mySndEqDict.get('local')
    backupSndEq = mySndEqDict.get('backup')
    messaggePrioritySndEq = mySndEqDict.get('messagePriority')
    blockIdentifier1SndEq = mySndEqDict.get('blockIdentifier1')
    blockIdentifier2SndEq = mySndEqDict.get('blockIdentifier2')
    applicationIdentifierSndEq = mySndEqDict.get('applicationIdentifier')
    inOutIdentifierSndEq = mySndEqDict.get('inOutIdentifier')
    serviceIdentifierSndEq =  mySndEqDict.get('serviceIdentifier')
    ltIdentifierSndEq = mySndEqDict.get('ltIdentifier')
    receiverAddressSndEq = mySndEqDict.get('receiverAddress')
    deliveryMonitoringSndEq = mySndEqDict.get('deliveryMonitoring')
    sessionNumberSndEq = mySndEqDict.get('sessionNumberSndEq')
    logging.writeLog("Load EQ config file successfully")
    try:
        #connect DB
        myConnect = oracleConnect.dbLogin(HOST, PORT, USER, PASSWORD, SERVICENAME, TABLESESSION, workFlow, market)
        transactionNumber, myTransaction = myConnect.connectDb() 
        #loop all day =))
        while True:
            sqlCheckConnect = "select count(transaction_no) from "+ USER + "." + TABLESESSION +" where transaction_no = '" + transactionNumber + "' and connect_end is null"
            myCursor = myTransaction.cursor()
            while True:
                try:
                    #check connection, if fail then reconnect
                    myTransaction = myConnect.reconnectDb(HOST, PORT, SERVICENAME, USER, PASSWORD)
                    myCursor.execute(sqlCheckConnect)
                    break
                except cx_Oracle.DatabaseError as myExcept:
                    error, = myExcept.args
                    logging.writeLog("Oracle-Error-Message: " + error.message)
                    time.sleep(1)
            workingProcess = myCursor.fetchone()[0]
            myCursor.close()
            logging.writeLog(str(workingProcess) + " " + workFlow + market + " process is working, looping, handling message")
            if workingProcess == 1:
                sleepTime = int(SLEEPTIME) #second
                #logging.writeLog("Handling VSD Messages")
                #__fileRun__ = str(workFlow) + str(market) + '.py'
                #print(__fileRun__)
                #exec(Path(__fileRun__).read_text())
                myThreadSndEq = sndeq.createThreadSndEq(USER, __VSDSNDEQ__, transactionNumber, myTransaction, localSndEq, backupSndEq, messaggePrioritySndEq, blockIdentifier1SndEq, blockIdentifier2SndEq, applicationIdentifierSndEq, inOutIdentifierSndEq, serviceIdentifierSndEq, ltIdentifierSndEq, receiverAddressSndEq, deliveryMonitoringSndEq, sessionNumberSndEq)
                returnSndEq = myThreadSndEq.execSndEq()
                #print (a)
                logging.writeLog("Plz wait for "+ str(sleepTime) +" second(s) til the next loop")
                time.sleep(sleepTime)
            else:
                logging.writeLog("Disconnection is required from Database")
                myConnect.disconnectDb(transactionNumber)
    except: 
        logging.writeLog("Exit reason: " + str(sys.exc_info()[0]))
    finally:
        myConnect.disconnectDb(transactionNumber)
        sys.exit()
    