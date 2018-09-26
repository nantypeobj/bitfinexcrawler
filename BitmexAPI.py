# -*- coding: utf-8 -*-
#PATH=r'C:\Users\Administrator.ZX-201609072125\Desktop\trading_backtest'
#import os,sys
#os.chdir(path=PATH)

import base
import pandas as pd
import api_settings as settings
from RESTAPI import RESTAPI
#import common.base as base
import datetime
import re
import time
import json
import base64
import hmac
import hashlib


class BitmexAPI(RESTAPI):
    """
    Client for the bitfinex.com API.
    See https://www.bitfinex.com/pages/api for API documentation.
    """
    
    def __init__(self,apiname='BITMEX'):
        key,secret=settings.APIKEY[apiname]
        super(BitmexAPI, self). __init__(key,secret,apiname,use_proxy=False)
    
    def get_data(self,url,method='GET',headers={},params={},verify=True):
        for i in range(3):
            try:
                return self.get(url,method=method,headers=headers,params=params,verify=verify)
            except Exception as e:
                msg=str(e).lower()
                if re.search('rate',msg):
                    self.use_proxy=(self.use_proxy==False)
                    try:
                        return self.get(url,method=method,headers=headers,params=params,verify=verify)
                    except Exception as e:
                        if re.search('rate',str(e).lower()):
                            time.sleep(60)
                            continue
                        else:
                            raise Exception(e)
                else:
                    raise Exception(e)
            else:
                break


    def klines(self,symbol,timeframe,period={'months':-1},
                      start=None,end=None,loop_gap=1):
        end=base.get_currenttime_asstr() if not end else end
        start=base.date_togapn(end,dateformat="%Y%m%d %H:%M:%S",**period) if start is None else start
        start,end=[int(base.str_toTimestamp(x)) for x in [start,end]]
        
        unit_val=int(re.findall('[0-9]+',timeframe)[0])
        if timeframe[-1]=='m':
            unit=60*unit_val
        elif timeframe[-1]=='h':
            unit=60*60*unit_val
        elif timeframe[-1]=='d':
            unit=60*60*24*unit_val
        else:
            unit=60*60*24*30*unit_val

        gap=(end-start)/(int(timeframe[:-1])*1000*(60 if timeframe[-1]=='m' else (60*60 if timeframe[-1]=='h' else 24*60*60)))
        gap=int(gap)
        klinedata=[]
        while gap>0:
            print(gap)
            limit=min(750,gap)
            if limit<1:
                return None
            #end=start+unit*limit*1000
            klinedata.append(self.candels(symbol,timeframe,start=base.timestamp_toStr(start,dateformat="%Y-%m-%d %H:%M:%S"),limit=limit))
            gap=gap-limit
            start=start+unit*limit*1000
            time.sleep(6)
        
        if len(klinedata)==0:
            return None
        else:
            return pd.concat(klinedata)


    #FRR	float	Flash Return Rate - average of all fixed rate funding over the last hour
    #BID	float	Price of last highest bid
    #BID_PERIOD	int	Bid period covered in days
    #BID_SIZE	float	Size of the last highest bid
    #ASK	float	Price of last lowest ask
    #ASK_PERIOD	int	Ask period covered in days
    #ASK_SIZE	float	Size of the last lowest ask
    #DAILY_CHANGE	float	Amount that the last price has changed since yesterday
    #DAILY_CHANGE_PERC	float	Amount that the price has changed expressed in percentage terms
    #LAST_PRICE	float	Price of the last trade
    #VOLUME	float	Daily volume
    #HIGH	float	Daily high
    #LOW	float	Daily low
    #timeframe: '1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D', '7D', '14D', '1M'
    def candels(self,symbol,timeframe,start,
                limit=750,return_url=False):
        """
        curl "https://www.bitmex.com/api/v1/trade/bucketed?binSize=1h&partial=false&symbol=XBT&count=100&reverse=false"
        """
        parameters={'binSize':timeframe,'symbol':symbol,'count':limit,'startTime':start}
        url=self.form_url(self.pathdict['KLINE'],
                      parameters=parameters,
                      version='v1')

        if return_url:
            return url
        
        res=self.get_data(url)
        return self.candels_format(res,symbol)
#        
    def candels_format(self,res,pair):
        try:
            df=pd.DataFrame(res,columns=['timestamp','symbol','open','high','low',
                                         'close','trades','volume','vwap','lastSize',
                                         'turnover','homeNotional','foreignNotional'])
            #df['time']=df['time'].apply(base.timestamp_toDatetime)
            df.columns=['time','pair','open','high','low','close','trades','volume','vwap','lastSize','turnover','homeNotional','foreignNotional']
            df['time']=[base.str_toTimestamp(t[:-5].replace('T',' '),dateformat="%Y-%m-%d %H:%M:%S") for t in df['time']]
            df.index=[base.timestamp_toDatetime(t) for t in df['time']]
            df['pct_change']=(df['close'].pct_change())*100.0

        except KeyError:
            return res
        return df


