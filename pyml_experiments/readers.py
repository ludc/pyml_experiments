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


    def read_logs(self):
        query = "select * from experiments limit 1"
        c = self.db.execute(query)
        columns = []
        for k in c.description:
            columns.append(k)

        logs={}
        for row in c:
            out={}
            for i in range(len(row)):
                c=columns[i]
                r=row[i]
                c=c.split(self.sep)

                curs=out
                for j  in range(len(c)-1):
                    if (not c[j] in curs):
                        curs[c[j]]={}
                    curs=curs[c[j]]
                curs[c[-1]]=r[-1]
            logs[curs["_id"]]=curs
        return logs
