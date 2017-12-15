import sqlite3
import pandas as pd
import numpy as np
import logging

class Reader(object):

    def read_experiments(self):
        raise NotImplementedError

    def read_logs(self,ids=[]):
        raise NotImplementedError

    def read_filtered_logs(self,filters=[],columns=[]):
        raise NotImplementedError

class Sqlite3Reader(Reader):
    def __init__(self,db_file=None,separator="."):
        logging.info("Opening Sqlite3 DB : "+db_file)
        self.db=sqlite3.connect(db_file)
        self.sep=separator


    def read_experiments(self):
        query = "select * from experiments"
        c = self.db.execute(query)
        columns = []
        for k in c.description:
            columns.append(k[0])

        logs={}
        for row in c:
            out={}
            for i in range(len(row)):
                cc=columns[i]
                r=row[i]
                cc=cc.split(self.sep)

                curs=out
                for j  in range(len(cc)-1):
                    if (not cc[j] in curs):
                        curs[cc[j]]={}
                    curs=curs[cc[j]]
                curs[cc[-1]]=r
            logs[curs["_id"]]=out
        return logs

    def read_log(self,id_experiment):
        query="select * from logs where _id=%d"%id_experiment
        c = self.db.execute(query)
        columns = []
        for k in c.description:
            columns.append(k[0])

        retour=[]
        for row in c:
            out={}
            for i in range(len(row)):
                cc=columns[i]
                r=row[i]
                cc=cc.split(self.sep)

                curs=out
                for j  in range(len(cc)-1):
                    if (not cc[j] in curs):
                        curs[cc[j]]={}
                    curs=curs[cc[j]]
                curs[cc[-1]]=r
            retour.append(out)
        return retour

    def filter_experiments(self,filter={}):
        def _match(doc,fil):
            if (type(doc)==dict):
                for k in fil:
                    if (k in doc):
                        m=_match(doc[k],fil[k])
                        if (not m):
                            return False
                return True
            else:
                return(doc==fil)


        l=self.read_experiments()
        reste=[]
        for k in l:
            v=l[k]
            if (_match(v,filter)):
                reste.append(k)
        return reste

    def restrict_log(self,id_experiment=None,columns={}):
        '''
        only keep particular fields in the log
        :param id_experiment:
        :param filter:
        :return:
        '''
        def _build(log,filter):
            current={}
            for k in filter:
                #assert k in log,"%s not a column in the log"%k
                if (k in log):
                    if (type(filter[k])==dict):
                        current[k]=_build(log[k],filter[k])
                    else:
                        current[k]=log[k]
                else:
                    logging.info("%s is not a filed in the log"%k)
            return current

        l=self.read_log(id_experiment)
        r=[]
        for ll in l:
            rr=_build(ll,columns)
            r.append(rr)
        return r

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

    def to_pandas(self,filter_experiments={},columns=None):
        all_lines=[]
        all_exps=self.read_experiments()
        exps=self.filter_experiments(filter_experiments)

        for e in exps:
            exp = self._flatten(all_exps[e])
            if (not columns is None):
                l=self.restrict_log(e,columns)
            else:
                l=self.read_log(e)

            for ll in l:
                ll=self._flatten(ll)
                line={}
                for ce in exp:
                    line[ce]=exp[ce]
                for ce in ll:
                    line[ce]=ll[ce]
                all_lines.append(line)

        all_columns={}
        iall_columns=[]
        retour=[]
        for l in all_lines:
            for k in l:
                if (not k in all_columns):
                    all_columns[k]=len(all_columns)
                    iall_columns.append(k)

        for l in all_lines:
            r=[]
            for i in range(len(iall_columns)):
                c=iall_columns[i]
                if (not c in l):
                    r.append(None)
                else:
                    r.append(l[c])
            retour.append(r)

        return pd.DataFrame(columns=iall_columns,data=retour)

