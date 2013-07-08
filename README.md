python_mapreduce
================

a multi-threading library

i use it in my light-weight spider program.

example:

<pre>
job = MapReduceJob(lambda p:(p[0],user.search(page=p[0])), 
                   lambda s1,s2:s1|s2,{i:i for i in xrange(1,100)},
                   mapper_count=10, 
                   reducer_count=2)
</pre>
job.start()
print job.output()
