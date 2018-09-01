# -*- coding: utf-8 -*-
import pandas as pd
import APIS.api_settings as settings
from APIS.RESTAPI import RESTAPI
import common.base as base
import datetime
import re
import time
import json
import base64
import hmac
import hashlib


class BitfinexAPI(RESTAPI):
    """
    Client for the bitfinex.com API.
    See https://www.bitfinex.com/pages/api for API documentation.
    """
    
    def __init__(self,apiname='BITFINEX'):
        key,secret=settings.APIKEY[apiname]
        super(BitfinexAPI, self). __init__(key,secret,apiname,use_proxy=True)
    
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
            
    def _sign_payload(self, payload):
        j = json.dumps(payload)
        data = base64.standard_b64encode(j.encode('utf8'))

        h = hmac.new(self.secret.encode('utf8'), data, hashlib.sha384)
        signature = h.hexdigest()
        return {
            "X-BFX-APIKEY": self.key,
            "X-BFX-SIGNATURE": signature,
            "X-BFX-PAYLOAD": data
        }
             
    
    def symbols(self):
        url=self.form_url(self.pathdict['SYMBOLS'],version='v1')
        res=self.get_data(url)
        return res
    
    def ticker(self,pair,return_url=False):
        """
        curl https://api.bitfinex.com/v2/ticker/Symbol
            [0.00039183,
             0.000321,
             30,
             ....]
        """
        
        url=self.form_url(self.pathdict['TICKER'] % pair,version='v2')

        if return_url:
            return url
        else:
            res=self.get_data(url)
            return self.ticker_format(res,pair)
#        
    def ticker_format(self,res,pair):
        try:
            dictionary=dict(zip(['bid','bid_size','ask','ask_size',
                                 'daily_change','daily_change_per',
                                 'last_price','volume','high','low'],res))
            dictionary['pair']=pair
        except KeyError:
            return res
        return dictionary
    
    
    def deals(self,pair,start=None,end=None,period={'months':-1},loop_gap=2):
        end=base.get_currenttime_asstr() if not end else end
        start=base.date_togapn(end,dateformat="%Y%m%d %H:%M:%S",**period) if start is None else start
        start,end=[int(base.str_toTimestamp(x)*1000) for x in [start,end]]
        

        dealdata=[]
        try:
            while start<end:
                    dealdf=self.trades(pair,start=start,end=end,limit=1000)
                    dealdata.append(dealdf)
                    end=time.mktime(dealdf.index[0].timetuple())*1000
                    time.sleep(loop_gap)
        except Exception:
            return pd.concat(dealdata)
        return pd.concat(dealdata) if dealdata else None
    
    
    
    
    def trades(self, pair,start=None,end=None,limit=50,sort=-1,
               period={'days':-1}): 
#        limit=min(int((end-start)/(int(timeframe[:-1])*1000*
#                        (60 if timeframe[-1]=='m' else (60*60 if timeframe[-1]=='h' else 24*60*60)))),1000)
        
        url=self.form_url(self.pathdict['TRADES'] % pair,
                      parameters={'start':start,'end':end,'sort':sort,'limit':limit},
                      version='v2')

        res=self.get_data(url)
        return  self.trades_format(res,pair)

    def trades_format(self,res,pair):
        try:
            df=pd.DataFrame(res,columns=['id','time','volume','price'])
            df['time']=df['time'].apply(base.timestamp_toDatetime)
            df['pair']=pair[1:]
            df=df.iloc[::-1]
            df.set_index('time',inplace=True)
            df=df.dropna()
        except KeyError:
            return res
        return df

    def klines(self,pair,timeframe,period={'months':-1},
                      start=None,end=None,loop_gap=1):
        end=base.get_currenttime_asstr() if not end else end
        start=base.date_togapn(end,dateformat="%Y%m%d %H:%M:%S",**period) if start is None else start
        start,end=[int(base.str_toTimestamp(x)) for x in [start,end]]
        
        unit_val=int(re.findall('[0-9]+',timeframe)[0])
        if timeframe[-1]=='m':
            unit=60*unit_val
        elif timeframe[-1]=='h':
            unit=60*60*unit_val
        elif timeframe[-1]=='D':
            unit=60*60*24*unit_val
        else:
            unit=60*60*24*30*unit_val

        gap=(end-start)/(int(timeframe[:-1])*1000*(60 if timeframe[-1]=='m' else (60*60 if timeframe[-1]=='h' else 24*60*60)))
        klinedata=[]
        while gap>0:
            limit=min(1000,gap)
            if limit<1:
                return None
            end=start+unit*limit*1000
            klinedata.append(self.candels(pair,timeframe,start=start,end=end,limit=limit))
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
    def candels(self,pair,timeframe,section='hist',period={'months':-1},f_paras=None,start=None,end=None,
                limit=100,sort=-1,return_url=False):
        """
        curl "https://api.bitfinex.com/v2/candles/trade:TimeFrame:Symbol/Section"
        """
        if pair[0]=='f':
            base.check_empty(f_paras,'period cant be empty when requesting funding market data')
            pair=pair+':'+f_paras
            
        if section=='hist':
            if not end:
                end=base.get_currenttime_asstr()
                end=int(base.str_toTimestamp(end)*1000)
            if not start:
                start=base.date_togapn(datetime.datetime.fromtimestamp(end/1000),dateformat="%Y%m%d %H:%M:%S",**period) 
                start=int(base.str_toTimestamp(start)*1000)
            limit=min(int((end-start)/(int(timeframe[:-1])*1000*
                        (60 if timeframe[-1]=='m' else (60*60 if timeframe[-1]=='h' else 24*60*60)))),1000)            
        url=self.form_url(self.pathdict['KLINE'] % (timeframe,pair,section),
                      parameters={'start':start,'end':end,'limit':limit,'sort':sort},
                      version='v2')

        if return_url:
            return url
        
        res=self.get_data(url)
        return self.candels_format(res,pair)
#        
    def candels_format(self,res,pair):
        try:
            df=pd.DataFrame(res,columns=['time','open','close','high','low','volume'])
            df['time']=df['time'].apply(base.timestamp_toDatetime)
            df['pair']=pair[1:]
            df=df.iloc[::-1]
            df.set_index('time',inplace=True)
            df['pct_change']=(df['close'].pct_change())*100.0
#            df['pct_accumulate_adj']=(df['close']/df['close'][0]-1)*100
#            df['pct_accumulate']=df['pct_change'].cumsum()
#            df['pct_change_vol']=((df['volume'].pct_change())*100.0)

#            df=df.dropna()
#            df['pct_change'] = preprocessing.scale(df['pct_change'])
            #df['pct_accumulate'] = preprocessing.scale(df['pct_accumulate'])

        except KeyError:
            return res
        return df

    def place_order(self, amount, price, side, ord_type,symbol='btcusd', 
                    exchange='bitfinex',select_fields=[]):
        """
        Submit a new order.
        :param amount:
        :param price:
        :param side:
        :param ord_type:
        :param symbol:
        :param exchange:
        :return:
        """
        url=self.form_url(self.pathdict['PORDER'],version='v1')
        payload = {

            "request": r"/"+r'v1/'+self.pathdict['PORDER'],
            "nonce": self._nonce,
            "symbol": symbol,
            "amount": amount,
            "price": price,
            "exchange": exchange,
            "side": side,
            "type": ord_type,
            'ocoorder':False

        }
        signed_payload = self._sign_payload(payload)

        res=self.get_data(url,'POST',headers=signed_payload,verify=True)
        return res

    def balances(self,select_fields=[]):
        """
        Fetch balances
        :return:
        """
        url=self.form_url(self.pathdict['BALANCE'],version='v1')
        payload = {

            "request": r"/"+r'v1/'+self.pathdict['BALANCE'],
            "nonce": self._nonce

        }
        signed_payload = self._sign_payload(payload)

        res=self.get_data(url,'POST',headers=signed_payload,verify=True)
        return self.balances_format(res)

    def balances_format(self,res):
        try:
            df=pd.DataFrame(res,columns=['amount','available','currency','type'])
        except KeyError:
            return res
        return df
