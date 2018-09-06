# -*- coding: utf-8 -*-
"""
Created on Thu Sep  6 08:45:59 2018

@author: Administrator
"""
from OKCoinFuture import OKCoinFuture

api=OKCoinFuture()
kline=api.future_kline('btc_usd','quarter',timeframe='1hour',since='20170101',size=5000)
print (kline.shape)
print (kline.head())

