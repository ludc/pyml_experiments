import logging
import sqlite3
import datetime
import sys

class Writer(object):
    def begin(self,experiment_name,arguments,device_id):
        raise NotImplementedError

    def write(self,values):
        raise NotImplementedError

    def exit(self):
        raise NotImplementedError

    def error(self,msg):
        raise NotImplementedError

    def _flatten(self,values,separator='.'):
        retour={}
        for k in values:
            if (type(values[k])==dict):
                f=self._flatten(values[k],separator=separator)
                for kk in f:
                    retour[k+separator+kk]=f[kk]
            elif(type(values[k])==list):
                retour[k]="\\n".join(values[k])
            else:
                retour[k]=values[k]
        return retour

    def _dclone(self,dic):
        r={}
        for k in dic:
            if (type(dic[k])==dict):
                r[k]=self._dclone(dic[k])
            else:
                r[k]=dic[k]
        return r


class StdoutWriter(Writer):
    def __init__(self):
        Writer.__init__(self)
        self._id=None
        self._values=[]

    def begin(self,experiment_name,arguments,device_id):
        logging.info("Starting experiment '%s'"%experiment_name)
        logging.info(str(arguments))
        logging.info(str(device_id))


    def write(self,values):
            logging.info("-- %s"%(str(values)))

    def exit(self):
        pass

    def error(self,msg):
        logging.error(msg)

class Sqlite3Writer(Writer):
    def __init__(self,db_file,update_every=1000):
        Writer.__init__(self)
        logging.info("Opening Sqlite3 DB : "+db_file)
        self.db=sqlite3.connect(db_file)
        self.update_every=update_every
        self._values=[]

    def _table_exists(self,name):
        query="select count(*) from sqlite_master where type='table' and name='%s'"%name
        cursor=self.db.execute(query)
        f=cursor.fetchone()
        return (f[0]>0)

    def _flatten_arguments_device(self,arguments,device_id):
        a=self._dclone(arguments)
        a["_device"]=device_id
        return(self._flatten(a))

    def _create_experiments_table(self,arguments,device_id):
        logging.info("== Creating experiments table...")
        arguments=self._flatten_arguments_device(arguments,device_id)
        arguments["_start_date"]=str(datetime.datetime.now())
        arguments["_end_date"]='unknown'
        arguments["_id"]=0
        arguments["_state"]="coucou"
        arguments["_error_msg"]="coucou"

        keys=[]
        for k in arguments:
            ty=type(arguments[k])
            if (ty==int):
                ty='INTEGER'
            elif(ty==float):
                ty='REAL'
            elif(ty==str):
                ty='TEXT'

            keys.append("'"+k+"' "+ty)

        query='create table experiments (%s);'%(",".join(keys))
        self.db.execute(query)
        return keys

    def _check_experiments_table(self,arguments,device_id):
        logging.info("== Checking experiments table...")
        arguments = self._flatten_arguments_device(arguments, device_id)
        query="select * from experiments limit 1"
        c=self.db.execute(query)
        columns={}
        for k in c.description:
            columns[k[0]]=1
        to_add=[]
        for k in arguments:
            if (not k in columns):
                ty = type(arguments[k])
                if (ty == int):
                    ty = 'INTEGER'
                elif (ty == float):
                    ty = 'REAL'
                elif (ty == str):
                    ty = 'TEXT'

                to_add.append("'"+k+"' "+ty)

        for k in to_add:
            logging.info("  -- adding column %s"%k)
            query="alter table experiments add column %s"%k
            self.db.execute(query)

    def _write_experiments_table(self,arguments,device_id):
        logging.info("== Writing experiments")
        arguments = self._flatten_arguments_device(arguments, device_id)
        arguments["_start_date"]=str(datetime.datetime.now())
        arguments["_state"]="running"
        arguments["_error_msg"]=""

        query="select max(_id) from experiments"
        value=self.db.execute(query).fetchone()
        if (value[0] is None):
            self._id=0
        else:
            self._id=value[0]+1
        arguments["_id"]=self._id
        att=[]
        args=[]
        for k in arguments:
            att.append("'"+k+"'")
            if (type(arguments[k])==str):
                args.append("'%s'"%arguments[k])
            else:
                args.append(str(arguments[k])   )

        query="insert into experiments(%s) values (%s)"%(",".join(att),",".join(args))
        self.db.execute(query)
        self.db.commit()


    def begin(self,experiment_name,arguments,device_id):
        #1: Check if tables exists
        if (not self._table_exists("experiments")):
            self._create_experiments_table(arguments,device_id)

        self._check_experiments_table(arguments,device_id)
        self._write_experiments_table(arguments,device_id)

    def _compute_values_keys(self):
        kk={}
        for v in self._values:
            for k in v:
                ty = type(v[k])
                if (ty == int):
                    ty = 'INTEGER'
                elif (ty == float):
                    ty = 'REAL'
                elif (ty == str):
                    ty = 'TEXT'
                kk[k]=ty
        r=[]
        for k in kk:
            r.append((k,kk[k]))
        return r


    def _create_logs_table(self):
        kk=self._compute_values_keys()
        kkk=[]
        for k1,k2 in kk:
            kkk.append("'"+str(k1)+"' "+str(k2))
        kkk.append("_id INTEGER")
        query="create table logs (%s)"%(",".join(kkk))
        self.db.execute(query)

    def _alter_logs_table(self):
        kk = self._compute_values_keys()
        query = "select * from logs limit 1"
        c = self.db.execute(query)
        columns = {}
        for k in c.description:
            columns[k[0]] = 1
        to_add=[]
        for k1,k2 in kk:
            if (not k1 in columns):
                logging.info("-- adding column in SQL Table: %s %s "%(k1,k2))
                query="alter table logs add column '%s' %s"%(k1,k2)
                self.db.execute(query)

    def _flush_values(self):
        if (not self._table_exists("logs")):
            self._create_logs_table()
        self._alter_logs_table()
        for v in self._values:
            keys = []
            vals=[]
            for k in v:
                val=v[k]
                if (type(val)==str):
                    val="'%s'"%val
                vals.append(str(val))
                keys.append("'%s'"%k)
            vals.append(str(self._id))
            keys.append("_id")

            query="insert into logs (%s) values (%s)"%(",".join(keys),",".join(vals))
            self.db.execute(query)
        self.db.commit()

    def write(self,values):
        self._values.append(self._flatten(values))
        if (len(self._values)==self.update_every):
            self._flush_values()
            self._values=[]





    def exit(self):
        self._flush_values()
        query = "update experiments set '_state'='done' where _id=%d" % self._id
        self.db.execute(query)
        query = "update experiments set '_end_date'='%s' where _id=%d" % (datetime.datetime.now(), self._id)
        self.db.execute(query)
        self.db.commit()

    def error(self,msg):
        query="update experiments set '_error_msg'='%s' where _id=%d"%(msg,self._id)
        self.db.execute(query)
        query="update experiments set '_state'='error' where _id=%d"%self._id
        self.db.execute(query)
        query="update experiments set '_end_date'='%s' where _id=%d"%(datetime.datetime.now(),self._id)
        self.db.execute(query)
        self.db.commit()

