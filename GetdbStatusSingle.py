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
from DBStatusChecker import DBStatusChecker

# import the logging library
import logging

def main(argv):
    '''
    getDBStatus = GetDBStatus("test_local.txt")
    getDBStatus.updateEventCount("2015-12-01", "2015-12-05")
    getDBStatus.updateVisitorCount("2015-12-01", "2015-12-05")
    getDBStatus.updateSessionCount("2015-12-01", "2015-12-05")
    getDBStatus.updateUserCount("2015-12-01", "2015-12-05")
    getDBStatus.updatePageviewCount("2015-12-01", "2015-12-05")
    '''
    process()
    
def process():
    parser = argparse.ArgumentParser(description = 'Possible tables:event,pageview,visitor,user,session')
    parser.add_argument("userfile", help="user config file")
    parser.add_argument("userfile_dst", help="user config file")
    parser.add_argument("start_date", help="start_date")    
    parser.add_argument("end_date", help="end_date")  
    parser.add_argument('-table', nargs = '?', const = 'all', default = 'all')
    args = parser.parse_args()
    userfile = args.userfile
    userfile_dst = args.userfile_dst
    dBStatusChecker = DBStatusChecker(userfile, userfile_dst)
    
    tableList= args.table
    dBStatusChecker.updateStatus(args.start_date, args.end_date, tableList)
        
if __name__ == '__main__':
        main(sys.argv)


    