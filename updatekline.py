# -*- coding: utf-8 -*-
"""
Created on Sun Sep  2 13:34:43 2018

@author: Administrator
"""
import sys
from BitmexAPI import BitmexAPI
from  BitfinexAPI import BitfinexAPI
from  OKCoinFuture import OKCoinFuture
from DatabaseInterface import DatabaseInterface
import base
import datetime
import time
import re

def initdata_from_okex(pair,timeframe,usedb):
    db=DatabaseInterface(usedb)
    api=OKCoinFuture()
    fpair,ftimeframe=format_input(pair,timeframe)
    kline=api.future_kline(pair,'quarter',timeframe=timeframe,period={'months':-3},size=5000)
    if not kline.empty:
        kline=kline[['high','low','open','close','time','volume','pct_change']]
        kline['pair']=fpair
        db.db_insertdataframe(kline,'KLINE'+ftimeframe)
        return True
    else:
        return False

def getkline_from_bitmex(pair,timeframe,exchangename,start,end=None):
    api=BitmexAPI()
    kline=api.klines(pair,timeframe,start=start,end=end)
    if not kline.empty:
        kline=kline[['high','low','open','close','time','volume','pct_change']]
        kline['pair']='BTCUSD' if pair=='XBTUSD' else pair
        print(kline.shape)
    print(kline[['close','high','low']])
    return kline

def getkline_from_bitfinex(pair,timeframe,exchangename,start,end=None):
    api=BitfinexAPI()
    if pair[0]!='t':
        pair='t'+pair
    res=api.klines(pair,timeframe,start=start,end=end)
#    print (res)
    print(res[['close','high','low']])
    print(pair+timeframe+str(start)+str(end))
    return res

def getkline_from_okex(pair,timeframe,exchangename,start):
    api=OKCoinFuture()
    kline=api.future_kline(pair,'quarter',timeframe=timeframe,since=start,size=5000)
    if not kline.empty:
        kline=kline[['high','low','open','close','time','volume','pct_change']]
        kline['pair']=pair.replace('_','').upper()
    return kline

#BITFINEX：获取K线
def getkline(pair,timeframe,exchangename,start,end=None):
    if exchangename=='bitfinex':
        return getkline_from_bitfinex(pair,timeframe,exchangename,start,end)
    elif exchangename=='okex':
        return getkline_from_okex(pair,timeframe,exchangename,start)
    elif exchangename=='bitmex':
        return getkline_from_bitmex(pair,timeframe,exchangename,start,end)
    else:
        print('输入的交易所不存在')


#更新已有的k线数据到最新
def updateklinedata(pair,timeframe,exchangename,usedb,rl,tf_asminute):
    #取当前数据库中最晚的一条数据的时间加上一秒为起始，获取到当前时间的数据#
    db=DatabaseInterface(usedb)
    fpair,ftimeframe=format_input(pair,timeframe,exchangename)
    lastrow=db.db_findone('KLINE'+ftimeframe,filter_dic={'pair':fpair},sel_fields=[],sort=[("time", -1)])      
    print(ftimeframe+fpair+usedb)
    if lastrow is None:
        if exchangename=='okex':
            return initdata_from_okex(pair,timeframe,usedb)
        else:
            print(ftimeframe+fpair+usedb)
            assert False,'数据库中没有找到数据'
    print(type(tf_asminute))
    if rl=='l':
        shiftsec=60*int(tf_asminute)*1000
    else:
        shiftsec=0
    lasttime=base.timestamp_toStr(float(lastrow['time'])+1000,dateformat="%Y%m%d %H:%M:%S")
    endtime=base.timestamp_toStr(time.time()*1000-shiftsec,dateformat="%Y%m%d %H:%M:%S") if rl=='l' else None
    #交易所获取k线函数
   # print(lasttime)
    print(lasttime+','+endtime)
    df=getkline(pair,timeframe,exchangename,lasttime,end=endtime)
    print (df.shape[0])
#    df=None
     #没有新数据，不更新
    if not df is None:
        #对于原始数据进行修正
        df['pct_change'].iloc[0]=(df['close'].iloc[0]-lastrow['close'])*100/lastrow['close']
#       if rl=='l':
#            df['time']=[t+shiftsec for t in df['time']]
        db.db_insertdataframe(df,'KLINE'+ftimeframe)
        return True
    else:
        return False

def updatesignal(pair,timeframe,usedb,exchangename=None):
    db=DatabaseInterface(usedb)
    pair,timeframe=format_input(pair,timeframe,exchangename)
    db.db_updateone({'timeframe':timeframe,'pair':pair},{'sig':'1'},'DATAUPDATESIG',upserts=True)

def format_input(pair,timeframe,exchangename=None):
    if exchangename=='bitmex' and pair=='XBTUSD':
        pair='BTCUSD'
    else:
        pair=pair.replace('_','').upper()
    tf1=re.findall(r'\d+', timeframe)[0]
    tf2=timeframe[timeframe.index(tf1[-1])+1]
    return pair,tf1+tf2


if __name__ == '__main__':
    #输入值
    #sys.argv[1]，pair，如：BTCUSD,EOSUSD,btc_usd....
    #sys.argv[2]，timeframe，如：30m,1hour....
    #sys.argv[3]，exchangename,bitfinex，okex
    #sys.argv[4], l,r
    #sys.srgv[5], timeframe as minute
    usedb='data_'+sys.argv[3].lower()
#    pair,timeframe='EOSUSD','30m'
    time.sleep(3)
    flg=updateklinedata(sys.argv[1],sys.argv[2],sys.argv[3],usedb,sys.argv[4],sys.argv[5])
    #print(sys.argv[1]+sys.argv[2]+sys.argv[3]+usedb)
    print('updating date at time'+str(datetime.datetime.now()))
    if flg: 
        print('data updated at time'+str(datetime.datetime.now()))
        updatesignal(sys.argv[1],sys.argv[2],usedb,sys.argv[3])
    else:
       print('empty data')
    #flg=updateklinedata('BTCUSD','1D',usedb)

 
