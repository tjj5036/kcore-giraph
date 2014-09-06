package org.apache.giraph.examples.kshell;

/* Giraph dependencies */
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.master.DefaultMasterCompute;

/* Hadoop dependencies */
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class KShellMasterCompute extends DefaultMasterCompute {
  /** Class logger */
  private static final Logger logger = Logger.getLogger(KShellMasterCompute.class);

  @Override
  public void initialize() throws InstantiationException,
      IllegalAccessException {
  }

  @Override
  public void compute() {

    /**
    * Corollary 1: Let K be the number of nodes with minimal degree in G.
    * Then the execution time on G is not larger than N-K+1 rounds.
    * Here, it is set to N rather than N-K+1, as this potentially
    * saves overhead of findiing the minimal degree in G, and then finding
    * the number of nodes with that degree.
    */
    if (getSuperstep() > getTotalNumVertices()) {
      logger.info("Algorithm terminated on superstep: " + getSuperstep());
      haltComputation();
    }
  }
}


