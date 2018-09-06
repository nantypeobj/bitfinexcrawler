# -*- coding: utf-8 -*-
"""
Created on Sun Sep  2 13:34:43 2018

@author: Administrator
"""
import sys
from  BitfinexAPI import BitfinexAPI
from DatabaseInterface import DatabaseInterface
import base
import datetime
import time

#BITFINEX：获取K线
def getkline(pair,timeframe,start,end=None):
    api=BitfinexAPI()
    if pair[0]!='t':
        pair='t'+pair
    res=api.klines(pair,timeframe,start=start,end=end)
#    if not res is None:
        #*为了避免在数据存储过程中timestamp的自动转换问题，将时间作为str保存
#        res['time']=[str(base.datetime_toTimestamp(t)) for t in res.index]
    return res


#更新已有的k线数据到最新
def updateklinedata(pair,timeframe,usedb):
    #取当前数据库中最晚的一条数据的时间加上一秒为起始，获取到当前时间的数据#
    
    db=DatabaseInterface(usedb)
    lastrow=db.db_findone('KLINE'+timeframe,filter_dic={'pair':pair},sel_fields=[],sort=[("time", -1)])
    if lastrow is None:
        assert False,'数据库中没有找到数据'
    lasttime=base.timestamp_toStr(float(lastrow['time'])+1,dateformat="%Y%m%d %H:%M:%S")
    #交易所获取k线函数
   # print(lasttime)
   # print(str(lastrow['time']))
    df=getkline(pair,timeframe,lasttime,end=None)
    print (df.shape[0])
#    df=None
     #没有新数据，不更新
    if not df is None:
        #对于原始数据进行修正
        df['pct_change'].iloc[0]=(df['close'].iloc[0]-lastrow['close'])*100/lastrow['close']
        db.db_insertdataframe(df,'KLINE'+timeframe)
        return True
    else:
        return False

def updatesignal(pair,timeframe,usedb):
    db=DatabaseInterface(usedb)
    db.db_updateone({'timeframe':timeframe,'pair':pair},{'sig':'1'},'DATAUPDATESIG',upserts=True)

if __name__ == '__main__':
    usedb='data_bitfinex'
#    pair,timeframe='EOSUSD','30m'
    time.sleep(30)
    flg=updateklinedata(sys.argv[1],sys.argv[2],usedb)
    print('updating date at time'+str(datetime.datetime.now()))
    if flg: 
    	print('data updated at time'+str(datetime.datetime.now()))
        updatesignal(sys.argv[1],sys.argv[2],usedb)
    else:
        print('empty data')
    #flg=updateklinedata('BTCUSD','1D',usedb)

  
