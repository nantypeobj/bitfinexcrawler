# -*- coding: utf-8 -*-

"""
Created on Fri Feb  9 08:42:51 2018

@author: Administrator
"""
#PATH=r'C:\Users\Administrator.ZX-201609072125\Desktop\cryptocurrency'
#import os,sys
#os.chdir(path=PATH)
#from APIS.BitfinexAPI
#from database.DatabaseInterface import DatabaseInterface
import crawl_config as conf
import time
from BitmexAPI import BitmexAPI
#import common.base as base
from BitfinexAPI import BitfinexAPI
from DatabaseInterface import DatabaseInterface
import base
from OKCoinFuture import OKCoinFuture

def crawldata(pair,timeframe,start,end,selectfields,contract_type=None,period=None):
    if conf.exchangename=='bitfinex':
        api=BitfinexAPI()
        func=api.klines
        funcpara={'pair':pair,'timeframe':timeframe,'start':start,'end':end}
    elif conf.exchangename=='bitmex':
        api=BitmexAPI()
        func=api.klines
        funcpara={'pair':pair,'timeframe':timeframe,'start':start,'end':end}
    elif conf.exchangename=='okex':
        api=OKCoinFuture()
        func=api.future_kline
        funcpara={'pair':pair,'timeframe':timeframe,'period':period,'contract_type':contract_type,'size':5000}
    else:
        assert False,'no such exchangename'

    while True:
        try:
            res=func(**funcpara)
            res['pair']=conf.pairname
        except IndexError:
            print ('时间段数据不存在，起始时间向后推迟一个月，重试...')
            start=base.date_togapn(start,dateformat="%Y%m%d %H:%M:%S",months=1)
            time.sleep(10)
            continue
        break
#    res['time']=[str(base.datetime_toTimestamp(t)) for t in res.index]
    res=res[selectfields]
    res=res.reset_index(drop=True)
    db.db_insertdataframe(res,conf.collnam)



if __name__ == '__main__':
#    api=BitfinexAPI()
#    api=BitmexAPI()
    db=DatabaseInterface(conf.usedb)
    pairs=[conf.pair]
#    allpairs=['t'+p.upper() for p in api.symbols() if ('usd' in p)]
#    allpairs=[p for p in allpairs if (not p in pairs)]
    selectfields=conf.selectfields
    timeframe=conf.timeframe
    contract_type=conf.contract_type
    period=conf.period                                                       
    start,end=conf.start,conf.end
    for p in pairs:
        crawldata(p,timeframe,start,end,selectfields,contract_type,period)
