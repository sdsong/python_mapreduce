'''
Created on Jun 20, 2013

@author: sdsong
'''
from mapreduce import MapReduceJob
import time
if __name__ == "__main__":
    for i in xrange(1,1000):
        job = MapReduceJob(lambda e:('result',e[1]**2), lambda e1,e2:e1+e2, {i:i for i in xrange(1,100)},5,5)
        start =  time.time()
        job.start()
        print job.output()
