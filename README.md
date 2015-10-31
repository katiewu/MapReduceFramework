# Map-Reduce Framework

>This is a simplified MapReduce framework implemented in Java. 

The framework consists of two types of nodes:
  - [x] One master
  - [x] Multiple workers
  
The master node assigns tasks to workers and coordinates among the workers; the workers are in charge of running map and reduce functions, and of storing the data. The master has a status page which displays the list of workers that are currently online and some information about each. My MapReduce framework also supports load balancing to avoid that some of the workers have already finished tasks but others are still busy.

## Instructions:
- [ ] The master and workers are implemented as Java Servlets. Load master servlet and worker servlets into different servlet containers (eg. Tomcat) on different machines.
- [ ] Set local storage directory for each worker and put file that you want to run on MapReduce framework in these storage directories.
- [ ] Launch servers and run master/workers servlets.
- [ ] Open the master status page, set Map/Reduce jobs, input directory, output directory, number of map threads and numbers of reduce threads, submit the mapreduce task.
- [ ] Check master's status page and get to know the progress of each worker.
- [ ] After finishing mapreduce task, check output files in output directory of workers.

