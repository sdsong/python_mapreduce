# -*- coding: utf-8 -*- 
'''
Created on Jun 17, 2013

@author: sdsong
'''
import Queue,threading,time,random

STOP_SIGNAL = None
print_lock = threading.Lock()

def safe_print(log):
    with print_lock:
        print log
def make_work_queue(size):
    work_queue = Queue.Queue(maxsize=size)
            
    def put(element):
        work_queue.put(element)
    def get():
        return work_queue.get()
    def qsize():
        return work_queue.qsize()         
    def not_full():
        return work_queue.not_full   
    return (put, get,qsize,not_full)  
class Mapper(threading.Thread):
    n_lock = threading.Lock()
    n = 0
    def __init__(self,func,job):
        threading.Thread.__init__(self)
        self.push,self.pop,self.qsize,self.not_full = make_work_queue(10)
        self.func = func
        self.job = job
        with Mapper.n_lock:
            Mapper.n = Mapper.n+1
            self.n = Mapper.n
        
    def run(self):
        while(True):
            task = self.pop()
            if task == STOP_SIGNAL:
                return
            self.job.push(self.func(task))
    def stop(self):
        return
    def __str__(self):
        return 'mapper %s -- qsize: %s'%(self.n,self.qsize())    
class Reducer(threading.Thread):
    n_lock = threading.Lock()
    n = 0 
    def __init__(self,func,job):
        threading.Thread.__init__(self)
        self.push,self.pop,self.qsize,self.not_full = make_work_queue(10)
        self.func = func
        self.job = job
        with Reducer.n_lock:
            Reducer.n = Reducer.n+1
            self.n =Reducer.n 
        self.result={}
    def run(self):
        while(True):  
            e1_pair = self.job.pop()
            if e1_pair == STOP_SIGNAL:
                return  
            e2 = self.result.get(e1_pair[0])

            if(e2 == None):
                self.result[e1_pair[0]] = e1_pair[1]
            else:
                self.result[e1_pair[0]] = self.func(e1_pair[1],e2)                        
          
    def stop(self):
        return   
    
    def __str__(self):
        return 'reducer %s -- qsize: %s'%(self.n,self.qsize())
class MapReduceJob:    
    def __init__(self,mapper,reducer,data,mapper_count = 5,reducer_count = 5,work_queue_size=10):              
        self.push,self.pop,self.qsize,self.not_full = make_work_queue(work_queue_size)
        self.data = data
        self.mapper,self.reducer = mapper,reducer
        self.mappers,self.reducers = [Mapper(mapper, self) for i in xrange(mapper_count)],[Reducer(reducer, self) for i in xrange(reducer_count)]
        
        self.result = {}
    def start(self):        
        for reducer in self.reducers:
            reducer.start()
        for mapper in self.mappers:
            mapper.start()

        while len(self.data)>0:
            for mapper in self.mappers:
                
                if len(self.data)>0:
                    task = self.data.popitem()
                    mapper.push(task)
        for mapper in self.mappers:
            mapper.push(None)
        for mapper in self.mappers:
            mapper.join()
            
        for reducer in self.reducers:
            self.push(None)    
 
        for reducer in self.reducers:
            for k,v in reducer.result.items():
                
                if self.result.has_key(k):
                    self.result[k] = self.reducer(self.result[k],v)
                else:
                    self.result[k] = v   
    def output(self):
        for reducer in self.reducers:
            reducer.join()
        return self.result
    
if __name__ == "__main__":
    job = MapReduceJob(lambda e:('result',e[1]**2), lambda e1,e2:e1+e2, {i:i for i in xrange(1,100)},4,2)
    start =  time.time()
    job.start()
    print job.output()

