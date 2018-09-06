# -*- coding: utf-8 -*-
"""
Created on Tue Oct  3 14:10:52 2017

@author: Administrator
"""

from decimal import Decimal
import datetime
from dateutil.parser import parse
import time
import pandas as pd

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