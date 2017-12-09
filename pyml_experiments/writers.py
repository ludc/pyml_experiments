import logging
import sqlite3
import datetime

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
                    retour[kk]=f[kk]
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


    def write(self,iteration,values):
            logging.info("At iteration %d : %s"%(iteration,str(values)))

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

            keys.append(k+" "+ty)

        query='create table experiments (%s);'%(",".join(keys))
        print(query)
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
                    ty == 'TEXT'

                to_add.append(k+" "+ty)

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
            att.append(k)
            if (type(arguments[k])==str):
                args.append("'%s'"%arguments[k])
            else:
                args.append(str(arguments[k])   )

        print(args)
        query="insert into experiments(%s) values (%s)"%(",".join(att),",".join(args))
        self.db.execute(query)


    def begin(self,experiment_name,arguments,device_id):
        #1: Check if tables exists
        if (not self._table_exists("experiments")):
            self._create_experiments_table(arguments,device_id)

        self._check_experiments_table(arguments,device_id)
        self._write_experiments_table(arguments,device_id)
        raise NotImplementedError

    def write(self,values):
        self._values.append(values)





    def exit(self):
        raise NotImplementedError

    def error(self,msg):
        raise NotImplementedError
