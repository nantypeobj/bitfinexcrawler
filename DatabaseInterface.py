# -*- coding: utf-8 -*-
"""
Created on Mon Apr 18 18:47:37 2016

@author: warriorzhai
"""
PATH=r'C:\Users\Administrator.ZX-201609072125\Desktop\trading_backtest'
import os,sys
os.chdir(path=PATH)
import sys
import pymongo
import time
import json
import pandas as pd
import database.db_settings as settings
import database.db_tables as tables
import common.base as base



class DatabaseInterface(object):
    
    def __init__(self,use_sb='btc'):
        self.db_url= settings.URLSET[use_sb]
        self.use_sb=use_sb
    
    def _db_connect(self,op,op_paras={}):
#        return self.cap.data_request(op,**op_paras)
        for i in range(3):
            try:
                res=op(**op_paras)
            except Exception as ex:
                print ('连接出错，第'+str(i)+'重试中......')
                time.sleep(settings.sleep_time)
                if i==(settings.try_times-1):
                    print ('操作失败')
                    raise Exception(ex)
                    
                    sys.exit()
                continue
            break
#        res=op(**op_paras)
        return res
    
    def db_insertdataframe(self,df,collnam):
        df = df[~df.index.duplicated(keep='first')]
        records = list(json.loads(df.T.to_json()).values())
        self.db_insertmany(records,collnam)

    def db_connect(self):
        op=pymongo.MongoClient
        op_paras={'host':self.db_url}
#        print (op_paras)
        client = self._db_connect(op,op_paras)
        db = client[self.use_sb]
        return db
    
    def db_insertone(self,data,collnam):
        coll=self.db_connect()[collnam]
        op=coll.insert_one
        op_para={'document':data}
        try:
            result=self._db_connect(op,op_para)
        except pymongo.errors.DuplicateKeyError:
            print ('表已存在项')
            return -1
        except pymongo.errors.InvalidDocument:
            print ('InvalidDocument')
            data={k:str(v) for k,v in data.items()}
            self.db_insertone(data,collnam)
        return result
    
    def db_insertmany(self,data,collnam):
        coll=self.db_connect()[collnam]
        op=coll.insert_many
        op_para={'documents':data}
        try:
            result=self._db_connect(op,op_para)
        except pymongo.errors.DuplicateKeyError:
            print ('表已存在项')
            return -1
        return result
        
    def db_insertarray_one(self,filter_dic,update_dic,collnam):
        update_id='$addToSet'
        result=self.db_updateone(filter_dic,update_dic,collnam,opr=update_id)
        return result
    
    def db_insertarray_many(self,filter_dic,update_dic,collnam):
        print ('insert many')
        update_id='$addToSet'
        update_val_id='$each'
        update_val_keys=[update_val_id]*len(update_dic)
        update_val_vals=[base.any_2list(o) for o in update_dic.values()]
        update=base.lists_2dict(update_dic.keys(),base.lists_2dictlists(update_val_keys,update_val_vals))
        result=self.db_updateone(filter_dic,update,collnam,opr=update_id)
        return result
    
    
    def db_updateone(self,filter_dic,update_dic,collnam,opr='$set',upserts=True):
        coll=self.db_connect()[collnam]
        op=coll.update_one
        op_para={'filter':filter_dic,'update':{opr:update_dic},'upsert':upserts}
        result=self._db_connect(op,op_para)
        return result
    
    def db_updatemany(self,filter_dic,update_dic,collnam,opr='$set',upserts=True):
        coll=self.db_connect()[collnam]
        op=coll.update_many
        
        key_list=[opr]*len(update_dic)
        update=base.lists_2dictlists(key_list,update_dic)
        
        op_para={'filter':filter_dic,'update':update,'upserts':upserts}
        result=self._db_connect(op,op_para)
        return result
    
    
        
    def db_findone(self,collnam,filter_dic={},sel_fields=[],sort=[]):
        coll=self.db_connect()[collnam]
        op=coll.find_one
        
        f_in=lambda x:{'$in':x} if base.isIter(x) else x
        op_para={}
        
        if filter_dic:
            filter_dic_vals=[f_in(v) for v in filter_dic.values()]
            op_para['filter']=base.lists_2dict(filter_dic.keys(),filter_dic_vals)        
        
        if sort:
            op_para['sort']=sort
        
        result=self._db_connect(op,op_para)
        
        if result and sel_fields:
            if base.isIter(sel_fields):
                try:
                    return [result[k] for k in sel_fields]
                except KeyError:
                    print ('request field does not exist...')
                    return None
    
            else:
                return result[sel_fields]
        else:
            return result
    
                
    
    def _db_find_format(self,dictiter,fields):
        fields=base.any_2list(fields)
        
        if  len(fields)==1:
            try:
                return base.unpack_dic(fields[0],dictiter)
            except KeyboardInterrupt:
                return base.unpack_dic(fields[0],dictiter)
        else:
            df=pd.DataFrame.from_dict(list(dictiter))
            df1 = df.where((pd.notnull(df)), None)
            if len(fields)==0:
                return df1
            else:
                return df1[fields]
        
    
    def db_find(self,sel_fields,collnam,filter_dic={},sort=None,limit=None):
        coll=self.db_connect()[collnam]
        op=coll.find
        
        f_in=lambda x:{'$in':x} if base.isIter(x) else x
        filter_dic_vals=[f_in(v) for v in filter_dic.values()]
        filter_dic_new=base.lists_2dict(filter_dic.keys(),filter_dic_vals)
        
        op_para={'filter':filter_dic_new}
        result=self._db_connect(op,op_para)
        
        if not sort is None:
            result=result.sort(sort)

        if not limit is None:
            result=result.limit(limit)
        
        return self._db_find_format(result,sel_fields)
    
    def _db_updateiter_count(self,filter_dicl,update_dicl,
                             collnam,update_func):
        #配置控制记录
        ctrl_filter_dic={'collnam': collnam}
        ctrl_update_dic={'step': 1}
        ctrl_updateover_dic={'step': 0}
        ctrl_findsel='step'
        ctrl_table_struct=tables.control_table_struct
        
        step=self.db_findone(ctrl_filter_dic,ctrl_findsel,ctrl_table_struct['collnam'])
        print ('断点开始于'+str(step))
        
        total_len=len(filter_dicl)-step
        process=0.0
        
        for f,u in zip(filter_dicl[step:],update_dicl[step:]):
            print ('更新数据.....')
            update_func(f,u,collnam)
            print ('更新计数.....')
            self.db_updateone(ctrl_filter_dic,ctrl_update_dic,ctrl_table_struct['collnam'],opr='$inc')
            process=process+1
            print ('已完成'+str(round(process*100/total_len,2))+'%.....')
        print ('数据更新完毕，重置计数器.....')
        self.db_updateone(ctrl_filter_dic,ctrl_updateover_dic,ctrl_table_struct['collnam'])
        
        return 0
    
    def db_updatemultiprocess(self,filter_dicl,update_dicl,collnam,
                              update_func=None,count=False):    
        if not update_func:
            update_func=self.db_updateone
        
        if count:
            self._db_updateiter_count(filter_dicl,update_dicl,
                             collnam,update_func)
        else:
            assert False,'unhandle'
#            p=MultiProcessTask()
#            task_funcsiterparas=iter([{'filter_dic':f,'update_dic':u} \
#                            for f in filter_dicl for u in update_dicl])
#            p.run_multiprocess(task_funcsiterparas=task_funcsiterparas,
#                            task_funcsconst=update_func,
#                            task_funcsconstparas={'collnam':collnam})
#            print ('update complete')
            
        return 0

    
#    def db_updateiter(self,filter_dicl,update_dicl,collnam,update_func=None,count=True):
#        if not update_func:
#            update_func=self.db_updateone
#        
#        if count:
#            self._db_updateiter_count(filter_dicl,update_dicl,
#                             collnam,update_func)
#        else:
#            total_len=len(filter_dicl)
#            process=0.0
#            for f,u in zip(filter_dicl,update_dicl):
#                print '更新数据.....'
#                update_func(f,u,collnam)
#                process=process+1
#                print '已完成'+str(round(process*100/total_len,2))+'%.....'
#            print '数据更新完毕'
#            
#        return 0
        
    def _db_insertiter_count(self,data,collnam):
        #配置控制记录
        ctrl_filter_dic={'collnam': collnam}
        ctrl_update_dic={'step': 1}
        ctrl_updateover_dic={'step': 0}
        ctrl_findsel='step'
        ctrl_table_struct=tables.control_table_struct
        
        step=self.db_findone(ctrl_filter_dic,ctrl_findsel,ctrl_table_struct['collnam'])
        print ('断点开始于'+str(step))
        
        total_len=len(data)-step
        process=0.0
        
        for x in data:
            print ('更新数据.....')
            self.db_insertone(x,collnam)
            print ('更新计数.....')
            self.db_updateone(ctrl_filter_dic,ctrl_update_dic,ctrl_table_struct['collnam'],opr='$inc')
            process=process+1
            print ('已完成'+str(round(process*100/total_len,2))+'%.....')
        print ('数据更新完毕，重置计数器.....')
        self.db_updateone(ctrl_filter_dic,ctrl_updateover_dic,ctrl_table_struct['collnam'])
        
        return 0
        
        
    def db_insertmultiprocess(self,data,collnam,count=False):    
        if not base.isIter(data) or type(data)==dict:
            self.db_insertone(data,collnam)
            
        if count:
            self._db_insertiter_count(data,collnam)
        else:
            assert False,'unhandel'
#            p=MultiProcessTask()
#            task_funcsiterparas=iter([{'data':dt} for dt in data])
#            p.run_multiprocess(task_funcsiterparas=task_funcsiterparas,
#                            task_funcsconst=self.db_insertone,
#                            task_funcsconstparas={'collnam':collnam})
#            print ('数据更新完毕')
            
        return 0
    
#    def db_insertiter(self,data,collnam,count=True):    
#        if not base.isIter(data) or type(data)==dict:
#            self.db_insertone(data,collnam)
#            
#        if count:
#            self._db_insertiter_count(data,collnam)
#        else:
#            total_len=len(data)
#            process=0.0
#            for x in data:
#                print '更新数据.....'
#                self.db_insertone(x,collnam)
#                process=process+1
#                print '已完成'+str(round(process*100/total_len,2))+'%.....'
#            print '数据更新完毕'
#            
#        return 0
    def db_remove(self,collnam,filter_dic={}):
        coll=self.db_connect()[collnam]
        op=coll.remove
        result=op(filter_dic)
        return result

    def db_count(self,collnam):
        coll=self.db_connect()[collnam]
        op=coll.count
        result=self._db_connect(op)
        return result
        
    def db_drop(self,collnam):
        coll=self.db_connect()[collnam]
        op=coll.drop
        result=self._db_connect(op)
        return result
    
    def db_ensure_index(self,collnam,indexnam,unique=True):
        coll=self.db_connect()[collnam]
        op=coll.ensure_index
        op_para={'key_or_list':indexnam,'unique':unique}
        result=self._db_connect(op,op_para)
        return result


class bulk(object):
    
    def __init__(self):
        pass




