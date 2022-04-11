import logging
import os
import time
import cx_Oracle
import json
from codecs import open

class createThreadSndEq:
    def __init__(self, user, __vsdsndeq__, transNo, myTrans, localSndEq, backupSndEq, messaggePrioritySndEq, blockIdentifier1SndEq, blockIdentifier2SndEq, applicationIdentifierSndEq, inOutIdentifierSndEq, serviceIdentifierSndEq, ltIdentifierSndEq, receiverAddressSndEq, deliveryMonitoringSndEq, sessionNumberSndEq):
        self.user = user
        self.__vsdsndeq__ = __vsdsndeq__
        self.transNo = transNo
        self.myTrans = myTrans
        self.localSndEq = localSndEq
        self.backupSndEq = backupSndEq
        self.messaggePrioritySndEq = messaggePrioritySndEq
        self.blockIdentifier1SndEq = blockIdentifier1SndEq
        self.blockIdentifier2SndEq = blockIdentifier2SndEq
        self.applicationIdentifierSndEq = applicationIdentifierSndEq
        self.inOutIdentifierSndEq = inOutIdentifierSndEq
        self.serviceIdentifierSndEq = serviceIdentifierSndEq
        self.ltIdentifierSndEq = ltIdentifierSndEq
        self.receiverAddressSndEq = receiverAddressSndEq
        self.deliveryMonitoringSndEq = deliveryMonitoringSndEq
        self.sessionNumberSndEq = sessionNumberSndEq
    def _selectRecordsToSend(self): 
        self.sqlRecordsToSend = "select reg_dt, seq_no, dat_cd, snd_dat, snd_tp, nvl(VSD_BRCH, '01') vsd_brch from " + self.user +"." + self.__vsdsndeq__ + " where snd_tp in ('1', '9') and proc_cnt < 6 order by reg_dt, snd_tp, seq_no"
        #print(self.sqlRecordsToSend)
        try:
            self.sndEqCursor = self.myTrans.cursor()
            self.sndEqCursor.execute(self.sqlRecordsToSend)
            workingRecords = self.sndEqCursor.fetchall()
            logging.writeLog('Working with Record(s): '+ str(self.sndEqCursor.rowcount))
        except:
            raise
        finally:
            self.sndEqCursor.close()
        return workingRecords
    def _createFileName(self, datCd ):
        try:
            self.fileNameCursor = self.myTrans.cursor()
            _bosSeq = self.fileNameCursor.var(cx_Oracle.NUMBER)
            self.fileNameCursor.callproc(self.user + ".pcom_nextseq", ["vsdsndeq_bos", _bosSeq])
            self.bosSeq = str(int(_bosSeq.getvalue(pos=0))).rjust(6, '0')
            self._fileName = "MT" + datCd + "_" + self.bosSeq + ".fin"
        except:
            raise
        finally:
            self.myTrans.commit()
            self.fileNameCursor.close()    
        return self._fileName, self.bosSeq
    def _createHeader(self, datCd, sessionNumber, bosSeq, vsdAddr):
        content1 = self.applicationIdentifierSndEq + self.serviceIdentifierSndEq + self.ltIdentifierSndEq + sessionNumber + bosSeq
        content2 = self.inOutIdentifierSndEq + datCd + self.receiverAddressSndEq + vsdAddr + self.messaggePrioritySndEq + self.deliveryMonitoringSndEq
        sndBlock1 = "{" + self.blockIdentifier1SndEq + ":" + content1 + "}"
        sndBlock2 = "{" + self.blockIdentifier2SndEq + ":" + content2 + "}"
        _header = sndBlock1 + sndBlock2
        return _header
    
    def _createBody(self, sndDat):
        _body = "{4:\n" + sndDat.decode().replace('\r','') + "\n-}"
        #print(sndDat)
        #dictionaryDat = json.load(open('./dictionary', 'r', 'utf-8')) # Load JSON dictionary
        with open('./dictionary', 'r', encoding = 'utf-8') as dictFile:
            dictionaryDat = json.load(dictFile)
        for vietnameseChar, vsdChar in dictionaryDat.items():
            _body = _body.replace(vietnameseChar, vsdChar)
        return _body
    
    def _createFooter(self):
        self._footer = "{5:{MAC:00000000}{CHK:F1DBCA886BBF}{TNG:}}"
        return self._footer

    def execSndEq(self):
        workingRecords = self._selectRecordsToSend()
        try:
            for record in workingRecords:
                mtFileSndEqName, bosSeq = self._createFileName(record[2])    
                logging.writeLog("Seq_no: "+ record[1] + ', '+ "Snd_tp: " + record[4]) 
                logging.writeLog(mtFileSndEqName)
                header = self._createHeader(record[2], self.sessionNumberSndEq, bosSeq, record[5])
                body = self._createBody(record[3].encode())
                footer = self._createFooter()
                content = header + body + footer
                logging.writeLog(content.encode('unicode-escape').decode('ascii'))
                #Create file in Send Folder
                mtFileSndEq = open(os.path.join(self.localSndEq, mtFileSndEqName), "w+")
                mtFileSndEq.write(content)
                mtFileSndEq.close()
                logging.writeLog("Finish create MT file(s) into SEND folder")
                #Backup Send file
                mtFileBckupSndEq = open(os.path.join(self.backupSndEq, mtFileSndEqName), "w+")
                mtFileBckupSndEq.write(content)
                mtFileBckupSndEq.close()
                logging.writeLog("Finish backup MT file(s) into BACKUP folder")
        except:
            logging.writeLog("Error while creating MT file(s)")
            raise
        return "1"