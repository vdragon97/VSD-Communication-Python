import cx_Oracle
import time
import socket
import platform
import hashlib 
import logging
import time
import sys
class dbLogin:
    def __init__(self, hostname, port, user, password, servicename, tablesession, workflow, market):
        self.hostname = hostname
        self.port = port
        self.user = user
        self.password = password
        self.servicename = servicename
        self.tablesession = tablesession
        self.workflow = workflow
        self.market = market
        
    def reconnectDb(self, hostname, port, servicename, user, password):
        dsnTns = cx_Oracle.makedsn(hostname,port,servicename)
        while True:
            try:
                myTransaction = cx_Oracle.connect(user,password,dsnTns)
                break
            except cx_Oracle.DatabaseError as myExcept:
                error, = myExcept.args
                logging.writeLog("Oracle-Error-Message: " + error.message)
                time.sleep(1)
        return myTransaction
        
    def connectDb(self):        
        self.myTransaction = self.reconnectDb(self.hostname, self.port, self.servicename, self.user, self.password)
        if len(str(self.myTransaction)) > 0:
            #logging.writeLog(self.myTransaction)
            logging.writeLog("---------------------------------")
            logging.writeLog("DATABASE INFO")
            logging.writeLog("User:          " + self.user)
            logging.writeLog("Host Name:     " + self.hostname)
            logging.writeLog("Port:          " + self.port)
            logging.writeLog("Service Name:  " + self.servicename)
            logging.writeLog("---------------------------------")
            self.myCursor = self.myTransaction.cursor()
        self.connectStartTime =  time.strftime("%Y%m%d%H%M%S")
        self.computerName = socket.gethostbyaddr(socket.gethostname())[0]
        self.ipAddr = str(socket.gethostbyaddr(socket.gethostname())[2]).split("'")[1]
        self.osVersion = platform.platform()
        self.transactionNumber = hashlib.md5((self.user + self.connectStartTime + self.ipAddr + self.computerName + self.osVersion + self.workflow + self.market).encode())
        self.transactionNumber = self.transactionNumber.hexdigest()
        self.sqlTransactionStart = "insert into " + self.user + "." + self.tablesession +"(user_id, connect_start, ip, computer_name, os_version, transaction_no, work_flow, market) values ('" + self.user + "', '" + self.connectStartTime + "', '"+ self.ipAddr + "', '" + self.computerName +"', '" + self.osVersion + "', '" + self.transactionNumber + "', '" + self.workflow + "', '" + self.market + "')"
        #self.bindVar = (self.user)
        #logging.writeLog(self.sqlTransactionStart)
        logging.writeLog("---------------------------------")
        logging.writeLog("LOGIN INFO")
        logging.writeLog("User:          " + self.user)
        logging.writeLog("Start Time :   " + self.connectStartTime)
        logging.writeLog("Computer Name: " + self.computerName)
        logging.writeLog("IP Address:    " + self.ipAddr)
        logging.writeLog("OS Version:    " + self.osVersion)
        logging.writeLog("Work-Flow:     " + self.workflow)
        logging.writeLog("Market:        " + self.market)
        logging.writeLog("Transaction N0:" + self.transactionNumber)
        logging.writeLog("---------------------------------")
        self.myCursor.execute(self.sqlTransactionStart)
        self.myTransaction.commit()
        return self.transactionNumber, self.myTransaction
        
    def disconnectDb(self, transactionNumber):
        try:
            self.connectEndTime =  time.strftime("%Y%m%d%H%M%S")
            self.sqlTransactionEnd = "update " + self.user + "." + self.tablesession +" set connect_end = '" + self.connectEndTime + "' where transaction_no = '" + self.transactionNumber + "'"
            #logging.writeLog(self.sqlTransactionEnd)
            logging.writeLog("---------------------------------")
            logging.writeLog("LOGOUT INFO")
            logging.writeLog("Transaction N0:" + self.transactionNumber)
            logging.writeLog("End Time :     " + self.connectEndTime)
            logging.writeLog("---------------------------------")
            self.myCursor.execute(self.sqlTransactionEnd)
            self.myTransaction.commit()
            self.myCursor.close()
            self.myTransaction.close()
        except cx_Oracle.DatabaseError:
            pass
         
    def executeSql(self, sql, bindVar = None, commit=False):
        try:
            self.myCursor.execute(sql, bindVar)
        except cx_Oracle.DatabaseError as e:
            # Log error as appropriate
            raise

        if commit:
            self.myTransaction.commit()     