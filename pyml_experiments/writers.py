import logging
import sqlite3
import datetime
import sys

class Writer(object):
    def begin(self,arguments):
        raise NotImplementedError

    def write(self,values):
        raise NotImplementedError

    def exit(self):
        raise NotImplementedError

    def error(self,msg):
        raise NotImplementedError



    def _dclone(self,dic):
        r={}
        for k in dic:
            if (type(dic[k])==dict):
                r[k]=self._dclone(dic[k])
            else:
                r[k]=dic[k]
        return r

class WriterWrapper(object):
    def __init__(self,writer1,writer2):
        self.w1=writer1
        self.w2=writer2

    def begin(self,arguments):
        self.w1.begin(arguments)
        self.w2.begin(arguments)

    def write(self,values):
        self.w1.write(values)
        self.w2.write(values)

    def exit(self):
        self.w1.exit()
        self.w2.exit()

    def error(self,msg):
        self.w1.error(msg)
        self.w2.error(msg)

class StdoutWriter(Writer):
    def __init__(self):
        Writer.__init__(self)
        self._id=None
        self._values=[]

    def begin(self,arguments):
        logging.info(str(arguments))


    def write(self,values):
            logging.info("-- %s"%(str(values)))

    def exit(self):
        pass

    def error(self,msg):
        logging.error(msg)

class Sqlite3Writer(Writer):
    def __init__(self,db_file,update_every=10,separator="."):
        Writer.__init__(self)
        logging.info("Opening Sqlite3 DB : "+db_file)
        self.db=sqlite3.connect(db_file)
        self.db_file=db_file
        self.update_every=update_every
        self._values=[]
        self.sep=separator
        self._iteration=0

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

    def _table_exists(self,name):
        query="select count(*) from sqlite_master where type='table' and name='%s'"%name
        cursor=self.db.execute(query)
        f=cursor.fetchone()
        return (f[0]>0)


    def _create_experiments_table(self,arguments):
        logging.info("== Creating experiments table...")
        args=self._flatten(arguments,separator=self.sep)
        args["_start_date"]=str(datetime.datetime.now())
        args["_end_date"]='unknown'
        args["_id"]=0
        args["_state"]="coucou"
        args["_error_msg"]="coucou"

        keys=[]
        for k in args:
            ty=type(args[k])
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

    def _check_experiments_table(self,arguments):
        logging.info("== Checking experiments table...")
        arguments = self._flatten(arguments,separator=self.sep)
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

    def _write_experiments_table(self,arguments):
        logging.info("== Writing experiments")
        arguments = self._flatten(arguments,separator=self.sep)
        arguments["_start_date"]=str(datetime.datetime.now())
        arguments["_state"]="running"
        arguments["_error_msg"]=""

        newconn=sqlite3.connect(self.db_file)
        newconn.isolation_level = 'EXCLUSIVE'
        newconn.execute('BEGIN EXCLUSIVE')

        query="select max(_id) from experiments"
        value=newconn.execute(query).fetchone()
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
        logging.info("Creating experiment with ID = %d"%self._id)
        query="insert into experiments(%s) values (%s)"%(",".join(att),",".join(args))
        newconn.execute(query)
        newconn.commit()
        newconn.close()

    def begin(self,arguments):
        #1: Check if tables exists
        if (not self._table_exists("experiments")):
            self._create_experiments_table(arguments)

        self._check_experiments_table(arguments)
        self._write_experiments_table(arguments)

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
        kkk.append("_iteration INTEGER")
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
        pos=0
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
            vals.append(str(self._iteration-len(self._values)+pos))
            keys.append("_iteration")
            pos+=1

            query="insert into logs (%s) values (%s)"%(",".join(keys),",".join(vals))
            self.db.execute(query)
        self.db.commit()

    def write(self,values):
        self._iteration+=1
        self._values.append(self._flatten(values,separator=self.sep))
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

