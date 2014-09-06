==============================
K-Core Decomposition in Giraph
==============================

Description
-----------

This is an implementation of K-Core Decomposition as outlined in the paper, 
`"Distributed k-Core Decomposition" <http://arxiv.org/pdf/1103.5320v2.pdf/>`_
as written by Alberto Montresor, Francesco De Pellegrini, and Daniele Miorandi.
This was written using the Giraph framework.
Currently, this implements the "one host one node" version as described in the paper.
Future work will include the "one host many nodes" version using the WorkerContext class,
which is currently empty at the moment.

Compiling (the cheap and easy way)
------------------------------------------

The easiest way to compile and run this code is to checkout this repository to your giraph-examples  src folder.
From there, simply build it by going to your $GIRAPH_HOME/giraph-examples and running:

  mvn compile (mvn flags like -DskipTests, etc)

Expected Graph Input
---------------------

Graphs are expected to be in the following JSON format:

  [vertex id 1, vertex value, [[neighbor, edge_value], [....]]

Currently, the vertex value is not used for initialization, but future implementations will probably use this.

Running
-------
After compiling, you can run the algorithm with (substitute the necessary variables):

  GRAPH=graph_name
  GRAPH_LOC=/path/to/graph
  $HADOOP_HOME/bin/hadoop dfs -rmr /user/hduser/output/kshell/
  $HADOOP_HOME/bin/hadoop dfs -rm /user/hduser/input/$GRAPH
  $HADOOP_HOME/bin/hadoop dfs -copyFromLocal $GRAPH_LOC/$GRAPH /user/hduser/input/$GRAPH
    

  $HADOOP_HOME/bin/hadoop jar $GIRAPH_HOME/giraph-examples/target/giraph-examples-your-version.jar \
      org.apache.giraph.GiraphRunner org.apache.giraph.examples.kshell.KShell \
      -mc org.apache.giraph.examples.kshell.KShellMasterCompute \
      -wc org.apache.giraph.examples.kshell.KShellWorkerContext \
      -vif org.apache.giraph.examples.kshell.KShellInputFormat \
      -vof org.apache.giraph.examples.kshell.KShellOutputFormat \
      -vip /user/hduser/input/$GRAPH \
      -op /user/hduser/output/kshell \
      -w number_of_workers) \
      /

Output is written in the JSON form:

  [vertex id 1, k-core value]
  [vertex id 2, k-core value]
