import datetime

class Experiment(object):

    def __init__(self,name="",arguments={},device_id={},writer=None):
        '''
        Create a new experiment given a name (string) and a set of arguments (dict)
        :param name:
        :param arguments:
        '''
        self.name=name
        self.arguments=arguments
        self.error_msg=None
        self.device_id=device_id
        self.iteration=0
        self.values={}
        self.current_scope=[]

        self.writer=writer
        self.writer.begin(experiment_name=name,arguments=arguments,device_id=device_id)

    def new_iteration(self):
        if (len(self.values)>0):
            self.writer.write(self.iteration,self.values)

        self.iteration+=1
        self.values={}

    def add_value(self,key,value):
        if (len(self.current_scope)==0):
            self.values[key]=value
        else:
            dic=self.values
            for i in range(len(self.current_scope)):
                cs=self.current_scope[i]
                if (not cs in dic):
                    dic[cs]={}
                dic=dic[cs]
            dic[key]=value

    def push_scope(self,scope):
        self.current_scope.append(scope)

    def pop_scope(self):
        del(self.current_scope[-1])

    def error(self,message):
        self.error_msg=message
        self.writer.error(message)

    def info(self,message):
        if (not "_messages" in self.values):
            self.values["_messages"]=[]

        self.values["_messages"].append(str(datetime.datetime.now())+" :: "+message)

    def __del__(self):
        self.writer.exit()



