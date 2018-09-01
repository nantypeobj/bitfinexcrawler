# -*- coding: utf-8 -*-
"""
Created on Tue Sep 19 16:29:41 2017

@author: Administrator
"""
#网络配置
TIMEOUT = 5.0
RETRY_TIMES=3
RETRY_DELAYS=2000   #每次重试之间间隔2s

#'BITFINEX':wa
#2FA认证;可API,可交易

#BITTREX:sa
#2FA认证;可API,可交易
#Key:207949e2e16146a3b05cf467d6978a66 
#Secret:0149ff35385745c49c6b00aa29df2733

#POLONIEX：wa
#2FA-EL7Y5FIOYGGMG4S2
#2FA认证;可API,可交易
#API Key	TGNZQH6T-PY8GYXEM-0MH6PECS-O1N5EJNX	Delete
#Secret	6cb653e4a921dfd8d4f2234fc04832a24e8ee02de5bfe41d5ec2245b48267f80dfe2070f1718e8a0ceaca8e32349641072e608bb1685ebc9bea3848ec643b6c4

#KRAKEN:wa
#tier1级别，金额有限制
#API Key:/JB+vBuemJkub226e004N4YrWXBxNIWtDBqJBfUIL+MxlHs0DcvyGoE5
#Secret：+DhgsEDo+EHLSprboBgqv3ijb3T0cuGkuvXSgMb1Qd/UFHpjfYLe+FlfaDwEwIKY62U/W6ggc5H9d/yysaS0Sg==

#CEX.IO:wa
#2FA-IVKGW5CXO47CK3SFO4VDOPSSHARUS4KL
#API Key:xIvDMEt14GIOG8mruLFboeSo
#Secret：KcNDrKG6R4N5agHTcwIPrE2TPY

#ITBIT：wa
#身份证认证、地址认证
#2FA-Z1jc-diM7-7Hv2-mjen-VRn1-dqvU
#API Key:4a00499f87fe3bb321f9460d99fba1de

#BITSTAMP：wa
#身份证认证、地址认证


#GEMINI:wa
#注册：加拿大、新加坡电话号码（中国不可以）；身份认证护照，地址认证


#GDAX：
#美国公民

#vps
#69.171.78.61
#root password:FnW50ZXyzj4v
#SSH Port:27616

SERVERS={
        'BITFINEX':'https://api.bitfinex.com'
        }

bitfinex_path_dict={
        'SYMBOL':'symbols',
        'CANDELS':'candles/trade:%s:%s/%s'
        }

PATH={
        'BITFINEX':bitfinex_path_dict
        }


