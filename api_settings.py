# -*- coding: utf-8 -*-
"""
Created on Tue Aug 29 11:15:17 2017

@author: Administrator
"""
import config

APIKEY=config.APIKEY

USERINFO_DICT=config.USERINFO_DICT

SERVERS={
'OKCOIN.CN':'https://www.okcoin.cn/api',
'OKEX':'https://www.okex.com/api',
'ZB':'http://api.zb.com/data',
'BIAN':'https://www.binance.com/api',
'BITFINEX':'https://api.bitfinex.com'

        }

PATH={
      'OKCOIN.CN':{'TICKER':"ticker.do",
                   'DEPTH':"depth.do"},
      'OKEX':{'TICKER':'future_ticker.do',
              'DEPTH':'future_depth.do',
              'TRADES':'future_trades.do',
              'RATE':'exchange_rate.do',
              'FKLINE':'future_kline.do',
              'KLINE':'kline.do'},
              
    'ZB':{'KLINE':'kline',
            },
    'BIAN':{'KLINE':'klines',
            'TICKERALL':'ticker/allPrices'
            },
    'BITFINEX':{
            'KLINE':'candles/trade:%s:%s/%s',
            'SYMBOLS':'symbols',
            'TICKER':'ticker/%s',
            'TRADES':'trades/%s/hist',
            'PORDER':'order/new',
            'BALANCE':'balances'
            }
      }

FEE_DICT={
'OKCOIN.CN':{
        'btc':0.2,
        'ltc':0.2,
        'eth':0.05,
        'etc':0.05,
        'bcc':0.01
        }
        }

CURRENCY_DICT={
        'OKCOIN.CN':['cny']
        }

API_TO_DB_FIELDS={
'OKCOIN.CN':{'CREATE_DATE': 'time', 
                          'ORDER_ID': 'orderid',
                          'DEAL_AMOUNT': 'deal_amount',
                          'AVG_PRICE': 'price',
                          'SYMBOL': 'pair',
                          'TYPE': 'type'
                          }
        }

POSITION_THRESHOLD=60