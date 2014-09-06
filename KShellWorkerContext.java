package org.apache.giraph.examples.kshell;

/* Giraph dependencies */
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;

/* Hadoop dependencies */
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/* Aggregator information */
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;

import java.io.IOException;

  /**
   * Worker context used with {@link SimplePageRankComputation}.
   */
  public class KShellWorkerContext extends WorkerContext {
  /** Class logger */
  private static final Logger logger = Logger.getLogger(KShellWorkerContext.class);

    @Override
    public void preApplication()
      throws InstantiationException, IllegalAccessException {
    }

    @Override
    public void postApplication() {
    }

    @Override
    public void preSuperstep() { 
      if (getSuperstep() >= 0) {
      }
    }

    @Override
    public void postSuperstep() { }
  }
