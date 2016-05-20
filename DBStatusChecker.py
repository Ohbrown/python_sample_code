import ConfigParser
import argparse
import sys
import MySQLdb
import httplib2
from apiclient.discovery import build
from oauth2client import file
import base64 
import pickle 

from apiclient.errors import HttpError
from apiclient import sample_tools
from oauth2client.client import AccessTokenRefreshError
from datetime import date
from datetime import time
from datetime import timedelta
from datetime import datetime

import datetime
import time
import pytz

# import the logging library
import logging

class DBStatusChecker(object):
    
     
    def __init__(self, file, file_dst):
        self.userfile = file
        self.userfile_dst = file_dst
        configParser = ConfigParser.RawConfigParser({'dbHost':'s2.ct5prtr59bmz.us-east-1.rds.amazonaws.com', 'dbPort':3306, 'dbUserName':'skyglue', 'dbPassword':'JosieAndrew', 'hasUserTable': 'no', 'hasTransaction': 'no'})
        configParser.read(self.userfile)  
        self.sourceUser = configParser.get('userData', 'dbUserName')
        self.sourceHost = configParser.get('userData', 'dbHost')
        self.source_port = configParser.get('userData', 'dbPort')
        self.sourcePWD = configParser.get('userData', 'dbPassword')
        self.userID = configParser.get('userData', 'path2')
        self.userDB = configParser.get('userData', 'path3')
        self.hasUserTable = configParser.get('userData', 'hasUserTable')
        self.hasTransaction = configParser.get('userData', 'hasTransaction')
        
        configParser_dst = ConfigParser.RawConfigParser()
        configParser_dst.read(self.userfile_dst)  
        self.dstUser = configParser_dst.get('userData', 'dbUserName')
        self.dstHost = configParser_dst.get('userData', 'dbHost')
        self.dst_port = configParser_dst.get('userData', 'dbPort')
        self.dstPWD = configParser_dst.get('userData', 'dbPassword')
        self.statusDB = configParser_dst.get('userData', 'statusDB')
        self.conn_sor, self.conn_dst =  self.getConnection()
        print "checking DB=" + self.userDB + " profileID=" + self.userID
            
    def updateStatus(self, start_date, end_date, tableList):
        #iterate through the date range
        s_date, interval = self.getTime(start_date, end_date)
        daysAfterStart = 0
        #call updateStatusSingleDay(date, tableList) 
        while(timedelta(days = daysAfterStart) <= interval):
            date = (s_date + timedelta(days = daysAfterStart)).strftime('%Y-%m-%d')
            self.intializeStatusRecord(date)
            daysAfterStart += 1
        self.updateStatusSingleDay(start_date, end_date, tableList) 
      
    def updateStatusSingleDay(self, start_date, end_date, tableList):
        #intializeStatusRecord(date) 
        try:
            # update status based on table list
            if tableList == "all" or tableList.find("event") != -1:
                self.updateEventCount(start_date, end_date)
        
            if tableList == "all" or tableList.find("pageview") != -1:
                self.updatePageviewCount(start_date, end_date)
        
            if tableList == "all" or tableList.find("session") != -1:
                self.updateSessionCount(start_date, end_date)
        
            if tableList == "all" or tableList.find("user") != -1:
                self.updateUserCount(start_date, end_date)
                
            if tableList == "all" or tableList.find("visitor") != -1:
                self.updateVisitorCount(start_date, end_date)
                
            if tableList == "all" or tableList.find("transaction") != -1:
                self.updateTransactionCount(start_date, end_date)
        finally:
            self.closeConnection()
     
    def intializeStatusRecord(self, date):
        #If the profileID+date is not in statusDB, insert them 
        cursor_d = self.conn_dst.cursor()
        result = cursor_d.execute("SELECT * FROM databaseData WHERE ga_profile_id = "+self.userID+" AND date = '"+date+"'")
        if(result == 0):
            cursor_d.execute("INSERT INTO databaseData(db_name, ga_profile_id, date) VALUES ('"+self.userDB+"','"+self.userID+"','"+date+"')")        
        cursor_d.close()
        self.conn_dst.commit()
              
    def updateEventCount(self, start_date, end_date):
        cursor_s = self.conn_sor.cursor()
        cursor_d = self.conn_dst.cursor() 
        result = cursor_s.execute("SELECT DATE(Date_Time), COUNT(*) FROM "+self.userID+"_Event where Date_Time>='"+start_date+" 00:00:00' AND Date_Time<='"+end_date+" 23:59:59' GROUP BY DATE(Date_Time);")
        event = cursor_s.fetchmany(result)
        for one_day_calc in event:
            cursor_d.execute("UPDATE databaseData SET event = "+str(one_day_calc[1])+" WHERE ga_profile_id = "+self.userID+" AND date = '"+str(one_day_calc[0])+"'")
        cursor_s.close()
        cursor_d.close()
        self.conn_sor.commit()
        self.conn_dst.commit()
        
        
    def updatePageviewCount(self, start_date, end_date):
        cursor_s = self.conn_sor.cursor()
        cursor_d = self.conn_dst.cursor()
        result = cursor_s.execute("SELECT DATE(Date_Time), COUNT(*) FROM "+self.userID+"_Pageviews where Date_Time>='"+start_date+" 00:00:00' AND Date_Time<='"+end_date+" 23:59:59' GROUP BY DATE(Date_Time);")
        pageview = cursor_s.fetchmany(result)
        for one_day_calc in pageview:
            cursor_d.execute("UPDATE databaseData SET pageview = "+str(one_day_calc[1])+" WHERE ga_profile_id = "+self.userID+" AND date = '"+str(one_day_calc[0])+"'")
        cursor_s.close()
        cursor_d.close()
        self.conn_sor.commit()
        self.conn_dst.commit()
        
    def updateSessionCount(self, start_date, end_date):
        cursor_s = self.conn_sor.cursor()
        cursor_d = self.conn_dst.cursor()
        result = cursor_s.execute("SELECT DATE(Visit_Date), COUNT(*) FROM `"+self.userID+"_Session` WHERE Visit_Date>='"+start_date+" 00:00:00' AND Visit_Date<='"+end_date+" 23:59:59' GROUP BY DATE(Visit_Date);")
        session = cursor_s.fetchmany(result)
        for one_day_calc in session:
            cursor_d.execute("UPDATE databaseData SET session = "+str(one_day_calc[1])+" WHERE ga_profile_id = "+self.userID+" AND date = '"+str(one_day_calc[0])+"'")
        cursor_s.close()
        cursor_d.close()
        self.conn_sor.commit()
        self.conn_dst.commit()
           
    
    def updateUserCount(self, start_date, end_date):
        cursor_s = self.conn_sor.cursor()
        cursor_d = self.conn_dst.cursor()
        if self.hasUserTable.lower() == "yes":
            result = cursor_s.execute("SELECT DATE(internalRecordDate), COUNT(*) FROM `"+self.userID+"_User` WHERE internalRecordDate >='"+start_date+" 00:00:00' AND internalRecordDate<='"+end_date+" 23:59:59' GROUP BY DATE(internalRecordDate);")
            user = cursor_s.fetchmany(result)
            for one_day_calc in user:
                cursor_d.execute("UPDATE databaseData SET user = "+str(one_day_calc[1])+" WHERE ga_profile_id = "+self.userID+" AND date = '"+str(one_day_calc[0])+"'")
            cursor_s.close()
            cursor_d.close()
        else:
            cursor_d.execute("UPDATE databaseData SET user = 9999 WHERE ga_profile_id = "+self.userID+" AND date >= '"+start_date+"' AND date <= '"+end_date+"'")
        self.conn_sor.commit()
        self.conn_dst.commit()
            
    def updateTransactionCount(self, start_date, end_date):
        cursor_s = self.conn_sor.cursor()
        cursor_d = self.conn_dst.cursor()
        if self.hasTransaction.lower() == "yes":
            result = cursor_s.execute("SELECT DATE(Visit_Date), COUNT(*) FROM `"+self.userID+"_Transaction` WHERE Visit_Date >='"+start_date+" 00:00:00' AND Visit_Date<='"+end_date+" 23:59:59' GROUP BY DATE(Visit_Date);")
            user = cursor_s.fetchmany(result)
            for one_day_calc in user:
                cursor_d.execute("UPDATE databaseData SET transaction = "+str(one_day_calc[1])+" WHERE ga_profile_id = "+self.userID+" AND date = '"+str(one_day_calc[0])+"'")
            cursor_s.close()
            cursor_d.close()
        else:
            cursor_d.execute("UPDATE databaseData SET transaction = 9999 WHERE ga_profile_id = "+self.userID+" AND date >= '"+start_date+"' AND date <= '"+end_date+"'")
        self.conn_sor.commit()
        self.conn_dst.commit()
            
        
    def updateVisitorCount(self, start_date, end_date):
        cursor_s = self.conn_sor.cursor()
        cursor_d = self.conn_dst.cursor()
        result = cursor_s.execute("SELECT DATE(internalRecordDate), COUNT(*) FROM `"+self.userID+"_Visitor` WHERE internalRecordDate >='"+start_date+" 00:00:00' AND internalRecordDate<='"+end_date+" 23:59:59' GROUP BY DATE(internalRecordDate);")
        visitor = cursor_s.fetchmany(result)
        for one_day_calc in visitor:
                cursor_d.execute("UPDATE databaseData SET visitor = "+str(one_day_calc[1])+" WHERE ga_profile_id = "+self.userID+" AND date = '"+str(one_day_calc[0])+"'")
        cursor_s.close()
        cursor_d.close()
        self.conn_sor.commit()
        self.conn_dst.commit()
         
    def getConnection(self):   
        conn_sor = MySQLdb.connect(host = self.sourceHost, 
                                   port = int(self.source_port), 
                                   user = self.sourceUser, 
                                   passwd = self.sourcePWD, 
                                   db = self.userDB)
        conn_sor.autocommit = True    
        
        conn_dst = MySQLdb.connect(host = self.dstHost, 
                               port = int(self.dst_port), 
                               user = self.dstUser, 
                               passwd = self.dstPWD, 
                               db = self.statusDB)
        conn_dst.autocommit = True 
        
        return conn_sor, conn_dst
    
    def closeConnection(self):
        self.conn_sor.close()
        self.conn_dst.close() 
        
    def getTime(self, start_date, end_date):
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        interval = end_date - start_date
        return start_date, interval
    
   