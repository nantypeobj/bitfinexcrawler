# -*- coding: utf-8 -*-
"""
Created on Mon Apr 18 19:03:14 2016

@author: warriorzhai
"""

import config


#数据库设定
try_times=3
sleep_time=5
server='127.0.0.1'
port='27017'

#数据库连接
DBINFO=config.DBINFO
URL='mongodb://%s%s%s%s%s:%s/%s%s%s'
URLSET={}
for nam,info in DBINFO.items():
    if info['USERID']:
        URLSET[nam]=URL % (info['USERID'],':',info['PWD'],'@',server,port,nam,'?authSource=',nam)
    else:
        URLSET[nam]=URL % ('','','','',server,port,nam,'','')


#
trades_collection='TRADES'
depth_collection='DEPTH'
