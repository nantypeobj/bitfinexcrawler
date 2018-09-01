# -*- coding: utf-8 -*-
"""
Created on Fri Apr 21 11:15:30 2017

@author: Administrator
"""
import settings 
from retrying import retry
import requests
import aiohttp
from decimal import Decimal
import datetime
from dateutil.parser import parse
import time
import config
import pandas as pd
import ssl
ssl.match_hostname = lambda cert, hostname: True

proxies=config.PROXY

class Connection(object):
    
    def __init__(self,use_proxy=False):
        self.use_proxy=use_proxy


    #===============重试相关配置=================
#    def retry_if_result_none(result):
#        """Return True if we should retry (in this case when result is None), False otherwise"""
#        return result is None
#    
#    @retry(retry_on_result=retry_if_result_none)
#    def might_return_none():
#        print "Retry forever ignoring Exceptions with no wait if return value is None"
    
#    def retry_if_io_error(exception):
#    """Return True if we should retry (in this case when it's an IOError), False otherwise"""
#    return isinstance(exception, IOError)
#
#    @retry(retry_on_exception=retry_if_io_error, wrap_exception=True)
#    def only_raise_retry_error_when_not_io_error():
#        print "Retry forever with no wait if an IOError occurs, raise any other errors wrapped in RetryError"

    #=============代理相关=====================
#    def set_proxy(self):
#        import socket
#        import socks
#        import socket
#        import socks
#        import requests
#        socks.set_default_proxy(socks.SOCKS5, "127.0.0.1",1081)
#        socket.socket = socks.socksocket
#        requests.get(r'http://www.google.com', timeout=24)
        
    #============网络请求========================
    @retry(stop_max_attempt_number=settings.RETRY_TIMES,wait_fixed=settings.RETRY_DELAYS)
    def request(self,url,method,headers={},params={},verify=False):
        otherpara={}
        if self.use_proxy:
            otherpara['proxies']=proxies
            print (otherpara['proxies'])
        if method=='GET':
            return requests.get(url,timeout=settings.TIMEOUT,verify=verify,**otherpara)
        else:
            return requests.post(url,data=params,headers=headers,verify=verify,**otherpara)  
    
    @retry(stop_max_attempt_number=2,wait_fixed=1)
    async def request_async(self,url,method='GET'):
        print ('....requesting url:'+url)
        res=await aiohttp.request(
                method, url)
        try:
            return await res.json()
        except aiohttp.ClientResponseError:
            return await res.text()
            
    
    def request_get(self,url,formattype='json'):
        print ('....requesting url:'+url)
        res=self.request(url,'GET')
        return self._request_format(res,formattype=formattype)
    
    def request_post(self,url,headers={},params={},formattype='json',verify=True):
        print ('....requesting url:'+url)
        
        if not headers:
            headers = {
                "Content-type" : "application/x-www-form-urlencoded",
         }
            
        res=self.request(url,'POST',headers=headers,params=params,verify=verify)
        return self._request_format(res,formattype=formattype)

    @retry(stop_max_attempt_number=settings.RETRY_TIMES,wait_fixed=settings.RETRY_DELAYS)
    def connect_api(self,apifunc,*args, **kwargs):
        return apifunc(*args, **kwargs)
    
    #============请求结果处理========================
    def _request_format(self,res,formattype='json'):
        if res.status_code!=200:
            raise Exception('fail code:'+str(res.status_code)+res.text)
        
        if formattype=='json':
            return res.json()
        elif formattype=='text':
            return res.text
        else:
            raise Exception('没有处理这种格式')
    
    async def _request_format_async(self,res,formattype='json'):
        if res.status_code!=200:
            raise Exception('fail code:'+str(res.status_code)+res.text)
        
        if formattype=='json':
            return await res.json()
        elif formattype=='text':
            return await res.text()
        else:
            raise Exception('没有处理这种格式')


class DataFormat(object):
    
    def __init__(self):
        pass

    
    def str_to_decimal(self,s):
        try:
            return Decimal(s)
        except:
            return s
    
    def str_to_float(self,s):
        try:
            return float(s)
        except:
            return s
        
    def col_to_int(self,col):
        return pd.to_numeric(col, downcast='integer',errors='coerce')
    
    def col_to_float(self,col):
        return pd.to_numeric(col, downcast='float',errors='coerce')
    
    def col_to_decimal(self,col):
        return col.applymap(lambda x: self.str_to_decimal(x))
    def col_to_datetime(self,col):
        return col.applymap(lambda x: self.str_to_decimal(x))
    
    
    def timestamp_to_datetime(self,t,dateformat="%Y%m%d %H:%M:%S"):
        return datetime.datetime.fromtimestamp(float(t)/1000)

    def str_to_timestamp(self,t):
        return self.datetime_to_timestamp(self.str_to_datetime(t))

    def str_to_datetime(self,t):
        return parse(t)

    def datetime_to_timestamp(self,t):
        return time.mktime(t.timetuple())
    
    def to_dataframe(self,v,colnams=None,index=None):
        try:
            return pd.DataFrame(v,columns=colnams,index=index)
        except ValueError:
            return pd.DataFrame(v,columns=colnams,index=[0])

    
#    def format_dict(self,dic,convertmap={},convertfunc=None):
#        if convertmap:
#            for k,f in convertmap.items():
#                dic[k]=eval(self._formfunc(f))(dic[k])
#        elif convertfunc:
#            for k,v in dic.items():
#                dic[k]=eval(self._formfunc(convertfunc))(dic[k])
#        else:
#            raise Exception('必须输入转化函数')
#        return dic
        
    
#    def format_column(self,df,convertmap={},convertfunc=None):
#        if convertmap:
#            for k,f in convertmap.items():
#                df[k]=df[k].apply(self._formfunc(f))
#        elif convertfunc:
#                df=df.apply(self._formfunc(f))
#        else:
#            raise Exception('必须输入转化函数')
#        return dic

