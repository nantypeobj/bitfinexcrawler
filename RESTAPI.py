# -*- coding: utf-8 -*-
"""
Created on Fri Sep 22 11:20:05 2017

@author: Administrator
"""
#import APIS.api_settings as settings
import pandas as pd
import api_settings as settings
from Connection import Connection
#from web.Connection import Connection
import time
#from database.DatabaseInterface import DatabaseInterface

class RESTAPI(object):
    
    def __init__(self,key,secret,exchange_name,use_proxy=False):
        self.server=settings.SERVERS[exchange_name]
        self.key=key
        self.secret=secret
        self.pathdict=settings.PATH[exchange_name]
        self.use_proxy=use_proxy

    #返回API方法请求地址
    #version--目前交易所有两个版本api，用version字段加以区分    
    def get(self, url,method='GET',*args,**kwards):
        conn=Connection(self.use_proxy)
        if method=='GET':
            return conn.request_get(url)
        else:
            return conn.request_post(url,*args,**kwards)
        
    def form_url(self,path,path_arg=None, parameters=None,version='v2'):
        # build the basic url
        url = "%s/%s/%s" % (self.server,version,path)

        # If there is a path_arh, interpolate it into the URL.
        # In this case the path that was provided will need to have string
        # interpolation characters in it, such as PATH_TICKER
        if path_arg:
            url = url % (path_arg)

        # Append any parameters to the URL.
        if parameters:
            url = "%s?%s" % (url, self._form_url_para(parameters))
        return url

    def _form_url_para(self, parameters):
        # sort the keys so we can test easily in Python 3.3 (dicts are not
        # ordered)
        keys = list(parameters.keys())
        keys.sort()
        return '&'.join(["%s=%s" % (k, parameters[k]) for k in keys])
    
    def _is_return_error(self,res):
        pass
    
    def format_result(self,res,grouped=False,convert_funcmap=None,*args,**kwards):
        pass  
    
    
    @property
    def _nonce(self):
        """
        Returns a nonce
        Used in authentication
        """
        return str(int(time.time() * 1000000))
