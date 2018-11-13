import sqlite3
import sys
import pickle
import pandas as pd

class PickleLogVisualizer(object):
    '''
    A small library to visualize simple curves based on SQL queries...
    '''
    def __init__(self,db_file,tbl_name="log"):
        self.db_file=db_file
        self.tbl_name=tbl_name



    def _sqlite_type(self,x):
        ty = type(x)
        if (x is None):
            return None

        if ty == int:
            return 'INTEGER'
        elif ty == float:
            return'REAL'
        elif ty == str:
            return 'TEXT'
        elif ty == bool:
            return 'TEXT'
        else:
            return "TEXT"

    def _compute_columns_names_and_types(self,x,prefix=""):
        coln=[]
        for k in x:
            if (type(x[k]) is dict):
                cc=self._compute_columns_names_and_types(x[k],k+"_")
                for ccc in cc:
                    coln.append((prefix+ccc[0],ccc[1]))
            else:
                v=x[k]
                ty=self._sqlite_type(v)
                coln.append((prefix+k,ty))
        return coln

    def _compute_columns_names_and_values(self,x,prefix=""):
        coln=[]
        for k in x:
            if (type(x[k]) is dict):
                cc=self._compute_columns_names_and_values(x[k],k+"_")
                for ccc in cc:
                    coln.append((prefix+ccc[0],ccc[1]))
            else:
                v=x[k]            
                coln.append((prefix+k,v))
        return coln

    def import_pickle_log_to_sqlite(self,filename):
        import progressbar
        f=open(filename,"rb")

        #First read and check columns
        col_and_types={}
        arguments={}
        nb_blocks=0
        while True:
            try:
                obj=pickle.load(f)
                
                
                if (type(obj) is dict):
                    if ("arguments" in obj):
                        arguments[obj["id"]]=obj["arguments"]                
                    obj=[obj]
                
                for o in obj:            
                    cnt=self._compute_columns_names_and_types(o)
                    for c,t in cnt:
                        if ((c in col_and_types) and (not t is None)):
                            if(col_and_types[c] is None):
                                col_and_types[c]=t
                            else:
                                assert(col_and_types[c]==t)
                        else:
                            col_and_types[c]=t
                nb_blocks+=1
            except (EOFError):
                break    
        f.close()
        self.columns_names_and_types=col_and_types

        db=sqlite3.connect(self.db_file)
        fields=[]
        for c in col_and_types:
            t=col_and_types[c]
            assert(not t is None)
            fields.append(c+" "+t)
        query="create table "+self.tbl_name+" ("+(",".join(fields))+");"
        db.execute(query)
        #Now, inserting
        f=open(filename,"rb")

        #First read and check columns
        col_and_types={}
        nb=0
        bar = progressbar.ProgressBar(maxval=nb_blocks, \
        widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
        bar.start()
        while True:
            try:
                obj=pickle.load(f)
                if(type(obj) is dict):
                    obj=[obj]

                for _obj in obj:
                    if ("values" in _obj):
                        _obj["arguments"]=arguments[_obj["id"]]
                    cnv=self._compute_columns_names_and_values(_obj)
                
                    cn=[]
                    vn=[]
                    for c,v in cnv:
                        cn.append(c)
                        if (v is None):
                            v="null"
                        else:
                            ty=self._sqlite_type(v)
                            if (ty=="TEXT"):
                                v='"'+str(v)+'"'
                        vn.append(str(v))
                    query="insert into "+self.tbl_name+"("+(",".join(cn))+") values ("+(",".join(vn))+");"
                    db.execute(query)
                nb+=1
                bar.update(nb)
            except (EOFError):
                break    
        f.close()    
        db.commit()
        db.close()
        bar.finish()

    def remove_null_lines(self,column_name):
        db=sqlite3.connect(self.db_file)
        query="delete from "+self.tbl_name+" where "+column_name+" is null;"
        db.execute(query)
        db.commit()
        db.close()

    def get_column_names(self):
        return self.columns_names_and_types

    def get_distinct_values(self,cn):
        query="select distinct("+cn+") from "+self.tbl_name+";"
        db=sqlite3.connect(self.db_file)
        cursor=db.execute(query)
        retour=[]
        for row in cursor:
            retour.append(row[0])
        return retour
        db.close()

    def execute_sqlite_query(self,q):
        db=sqlite3.connect(self.db_file)
        db.execute(q)
        db.commit()
        db.close()

    def get_pandas(self,columns_names,query):
        db=sqlite3.connect(self.db_file)
        cursor=db.execute(query)
        vals=[]
        for row in cursor:
            vals.append(row)
        retour=pd.DataFrame(vals,columns=columns_names)
        db.commit()
        db.close()
        return retour
    

# pp=PickleLogVisualizer(sys.argv[2])
# pp.import_pickle_log_to_sqlite(sys.argv[1])
# print(pp.get_column_names())
# print(pp.get_distinct_values("arguments_learning_rate"))
# pp.remove_null_lines("arguments_learning_rate")
# print(pp.get_distinct_values("arguments_learning_rate"))
# df=pp.get_pandas(["reward","id"],"select id,avg(values_reward) from log group by id,_iteration")
# df.plot()

