# -*- coding: utf-8 -*-
"""
Created on Fri Aug 25 16:24:02 2017

@author: Administrator
"""

#!/usr/bin/python
# -*- coding: utf-8 -*-
#用于访问OKCOIN 期货REST API
from RESTAPI import RESTAPI
import pandas as pd
import time
import datetime
from dateutil.parser import parse
from decimal import Decimal
from dateutil.relativedelta import relativedelta
import api_settings as settings
from DataFormat import DataFormat
import  base

fmt=DataFormat()

class OKCoinFuture(RESTAPI):

    def __init__(self,apiname='OKEX'):
        key,secret=settings.APIKEY[apiname]
        super(OKCoinFuture, self). __init__(key,secret,apiname,use_proxy=False)        
    def _format(self,res,*args,**kwards):
        res=self.fmt.to_dataframe(res,*args,**kwards)
        res.columns=[s.upper()  for s in res.columns]
        return res
    
    #OKCOIN期货行情信息
    #contract_type合约类型: this_week:当周   next_week:下周   quarter:季度
    def future_ticker(self,symbol,contract_type,return_url=False):
        url=self.form_url(self.pathdict['TICKER'],
                      parameters={'symbol':symbol,'contract_type':contract_type},
                      version='v1')
        if return_url:
            return url
        
        res=self.get(url)
        return self.ticker_format(res)
    
    def ticker_format(self,res):
        try:
            data=res['ticker']
            data['time']=res['date']
            data['rate']=self.exchange_rate()
        except KeyError:
            return res
        return {k:fmt.str_to_float(v) for k,v in data.items() if not k in ['contract_id','time'] }

    
    #OKCoin期货市场深度信息
    #size Integer value: 1-200
    def future_depth(self,symbol,contract_type,size=20,return_url=False): 
        url=self.form_url(self.pathdict['DEPTH'],
                      parameters={'symbol':symbol,'contract_type':contract_type,
                                  'size':size},
                      version='v1')
        if return_url:
            return url
        
        res=self.get(url)
        return self.depth_format(res)

    def depth_format(self,res):
        print ('formating depth data...')
        try:
            askdf=pd.DataFrame(res['asks'],columns=['ask','ask_volume'])
            biddf=pd.DataFrame(res['bids'],columns=['bid','bid_volume'])
        except KeyError:
            return res
        askdf=askdf.apply(lambda x:pd.to_numeric(x,downcast='float'))
        biddf=biddf.apply(lambda x:pd.to_numeric(x,downcast='float'))
        now=str(base.nounce())
        askdf['time']=[now]*biddf.shape[0]
        biddf['time']=[now]*biddf.shape[0]
        return askdf,biddf

    #OKCoin期货交易记录信息
    def future_trades(self,symbol,contract_type,return_url=False):
        url=self.form_url(self.pathdict['TRADES'],
                      parameters={'symbol':symbol,'contract_type':contract_type},
                      version='v1')
        if return_url:
            return url
        
        res=self.get(url)
        return self.trades_format(res)

    def trades_format(self,res):
        try:
            df=pd.DataFrame(res)
        except KeyError:
            return res
        df['date']=df['date'].astype(str)
        df['date_ms']=df['date_ms'].astype(str)
        df['tid']=df['tid'].astype(str)
        return df


#    #OKCoin期货指数
#    def future_index(self,symbol,return_url=False):
#        FUTURE_INDEX = "/api/v1/future_index.do"
#        params=''
#        if symbol:
#            params = '?symbol=' +symbol+'_usd'
#        url=self.__url+FUTURE_INDEX +params
#        if return_url:
#            return url
#        index=self.conn.request_get(url)['future_index']
#        return index*self.exchange_rate()

    #获取美元人民币汇率
    def exchange_rate(self):
        url=self.form_url(self.pathdict['RATE'],
                      version='v1')
        return self.get(url)['rate']
    

#    #获取预估交割价
#    def future_estimated_price(self,symbol):
#        FUTURE_ESTIMATED_PRICE = "/api/v1/future_estimated_price.do"
#        params=''
#        if symbol:
#            params = 'symbol=' +symbol
#        return httpGet(self.__url,FUTURE_ESTIMATED_PRICE,params)
#    
    #OKCoin期货指数
    def future_kline(self,pair,contract_type,timeframe='1day',size=1000,
                     since='',period={'months':-1},return_url=False):
        if since:
            since=int(base.str_toTimestamp(since))
        elif period:
            since=int(time.mktime((datetime.datetime.now()+relativedelta(**period)).timetuple()))*1000
        
        columns=['time','open','high','low','close','volume',
                                         'volume_as_asset']
        
        url=self.form_url(self.pathdict['FKLINE'],
                      parameters={'symbol':pair,'type':timeframe,'contract_type':contract_type,
                                  'since':since,'size':size},
                      version='v1')
        if return_url:
            return url
        
        res=self.get(url)
        return self.kline_format(res,pair,columns)

    def kline(self,pair,timeframe='1day',size=1000,
                     since='',period={'months':-1},return_url=False):
        
        columns=['time','open','high','low','close','volume']
        
        if since:
            since=int(base.str_toTimestamp(since))*1000
        elif period:
            since=int(time.mktime((datetime.datetime.now()+relativedelta(**period)).timetuple()))*1000
        url=self.form_url(self.pathdict['KLINE'],
                      parameters={'symbol':pair,'type':timeframe,
                                  'since':since,'size':size},
                      version='v1')
        if return_url:
            return url
        
        res=self.get(url)
        return self.kline_format(res,pair,columns)

   
    def kline_format(self,res,pair,columns):
        try:
            df=pd.DataFrame(res,columns=columns)
            df.index=df['time'].apply(base.timestamp_toDatetime)
            df['pair']=pair
            df['pct_change']=df['close'].pct_change()*100
            df['pct_accumulate']=df['pct_change'].cumsum()

        except KeyError:
            return res
        return df
#        
#
#    #期货全仓账户信息
#    def future_userinfo(self):
#        FUTURE_USERINFO = "/api/v1/future_userinfo.do?"
#        params ={}
#        params['api_key'] = self.__apikey
#        params['sign'] = buildMySign(params,self.__secretkey)
#        return httpPost(self.__url,FUTURE_USERINFO,params)
#
#    #期货全仓持仓信息
#    def future_position(self,symbol,contractType):
#        FUTURE_POSITION = "/api/v1/future_position.do?"
#        params = {
#            'api_key':self.__apikey,
#            'symbol':symbol,
#            'contract_type':contractType
#        }
#        params['sign'] = buildMySign(params,self.__secretkey)
#        return httpPost(self.__url,FUTURE_POSITION,params)
#
#    #期货下单
#    def future_trade(self,pair,contractType,price='',amount='',tradeType='',matchPrice='',leverRate=''):
#        FUTURE_TRADE = "/api/v1/future_trade.do?"
#        params = {
#            'api_key':self.__apikey,
#            'symbol':pair,
#            'contract_type':contractType,
#            'amount':amount,
#            'type':tradeType,
#            'match_price':matchPrice,
#            'lever_rate':leverRate
#        }
#        if price:
#            params['price'] = price
#        params['sign'] = buildMySign(params,self.__secretkey)
#        url=self.__url+FUTURE_TRADE
#        res=self.conn.request_post(url,params=params)
#        return res
#
#    #期货批量下单
#    def future_batchTrade(self,symbol,contractType,orders_data,leverRate):
#        FUTURE_BATCH_TRADE = "/api/v1/future_batch_trade.do?"
#        params = {
#            'api_key':self.__apikey,
#            'symbol':symbol,
#            'contract_type':contractType,
#            'orders_data':orders_data,
#            'lever_rate':leverRate
#        }
#        params['sign'] = buildMySign(params,self.__secretkey)
#        return httpPost(self.__url,FUTURE_BATCH_TRADE,params)
#
#    #期货取消订单
#    def future_cancel(self,symbol,contractType,orderId):
#        FUTURE_CANCEL = "/api/v1/future_cancel.do?"
#        params = {
#            'api_key':self.__apikey,
#            'symbol':symbol,
#            'contract_type':contractType,
#            'order_id':orderId
#        }
#        params['sign'] = buildMySign(params,self.__secretkey)
#        return httpPost(self.__url,FUTURE_CANCEL,params)
#
#    #期货获取订单信息
#    def future_orderinfo(self,symbol,contractType,orderId,status,currentPage,pageLength):
#        FUTURE_ORDERINFO = "/api/v1/future_order_info.do?"
#        params = {
#            'api_key':self.__apikey,
#            'symbol':symbol+'_usd',
#            'contract_type':contractType,
#            'order_id':orderId,
#            'status':status,
#            'current_page':currentPage,
#            'page_length':pageLength
#        }
#        url=self.__url+FUTURE_ORDERINFO
#        res=self.conn.request_post(url,params=params)
##        data=self._format(res['orders'])
#        return res
#    
#    #期货逐仓账户信息
#    def future_userinfo_4fix(self):
#        FUTURE_INFO_4FIX = "/api/v1/future_userinfo_4fix.do?"
#        params = {'api_key':self.__apikey}
#        params['sign'] = buildMySign(params,self.__secretkey)
#        url=self.__url+FUTURE_INFO_4FIX
#        res=self.conn.request_post(url,params=params)
##        data=self._format(res['orders'])
#        return res
#
#    #期货逐仓持仓信息
#    def future_position_4fix(self,symbol,contractType,type1):
#        FUTURE_POSITION_4FIX = "/api/v1/future_position_4fix.do?"
#        params = {
#            'api_key':self.__apikey,
#            'symbol':symbol,
#            'contract_type':contractType,
#            'type':type1
#        }
#        params['sign'] = buildMySign(params,self.__secretkey)
#        return httpPost(self.__url,FUTURE_POSITION_4FIX,params)
#
#    #个人账户资金划转
#    #type 划转类型。1：现货转合约 2：合约转现货
#    #amount 划转币的数量
#    def future_devolve(self,symbol,trans_type,amount):
#        FUTURE_DEVOLVE= "/api/v1/future_devolve.do?"
#        params = {
#            'api_key':self.__apikey,
#            'symbol':symbol,
#            'type':trans_type,
#            'amount':amount
#        }
#        params['sign'] = buildMySign(params,self.__secretkey)
#
#        url=self.__url+FUTURE_DEVOLVE
#        res=self.conn.request_post(url,params=params)
#        return res
#





    
