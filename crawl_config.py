# -*- coding: utf-8 -*-
"""
Created on Mon Mar  5 19:02:00 2018

@author: Administrator
"""
#PATH=r'C:\Users\Administrator.ZX-201609072125\Desktop\cryptocurrency'
#import os,sys
#os.chdir(path=PATH)

import base
#===历史数据抓取配置===========
#--抓取币种
#tBTCUSD,tLTCUSD,tETHUSD,XRPUSD,EOSUSD,NEOUSD,IOTUSD,OMGUSD,BCHUSD,ETPUSD,tSNTUSD,ETCUSD
#'tSANUSD','tQTMUSD','tEDOUSD','tDATUSD','tAVTUSD','tQSHUSD','tYYWUSD'
#       'tGNTUSD','tZRXUSD','tTRXUSD','tELFUSD'
pair='XBT'
pais=['tBATUSD','tMNAUSD',
       'tFUNUSD','tTNBUSD','tSPKUSD','tRCNUSD','tRLCUSD']
pairname='BTCUSD'
#'tBATUSD','tMNAUSD',
#       'tFUNUSD','tTNBUSD','tSPKUSD','tRCNUSD','tRLCUSD']

#--抓取tf
timeframe='5m'
#--抓取时间段
#start,end='20180616','20180715'
start,end='2017-01-01','2018-09-11'
start_period={}
end_period={}

#检查/计算时间
if not end:
    end=base.get_currenttime_asstr()

if not start:
    start=base.date_togapn(end,dateformat="%Y%m%d %H:%M:%S",**end_period)

selectfields=['time','pair','open','high','low','close','volume','pct_change']

#====数据库配置
usedb='data_bitmex'
collnam='KLINE'+timeframe




