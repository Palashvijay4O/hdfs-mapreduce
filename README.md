# hdfs-mapreduce

Implementation of a distributed file system similar to HDFS. On top of it, implemented a MapReduce framework with distributed algorithm that runs on cluster of 4 machines. 
The code uses Java RMI for communication among different machines/nodes and Google Protobuf is used for marshalling and unmarshalling of objects for message passing.

NameNode maintains all the metadata of the hdfs.
DataNode stores the blocks of data where each block can be present of one of the 3 data nodes.
Client can issue the put, get or list command to hdfs.

JobTracker assigns the mappers and reducers to the TaskTrackers.
TaskTrackers are multi-threaded nodes which runs mappers and reducers on the blocks of data and put the mappers and reducers output files back to hdfs.

The bash files are provided inside each of the src folder for running the code with ease.
