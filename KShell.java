package org.apache.giraph.examples.kshell;
import org.apache.giraph.examples.kshell.KShellMessage;
import org.apache.giraph.examples.kshell.KShellVertexValue;

/* Giraph dependencies */
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.LongWritable;

/* Hadoop dependencies */
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;

/**
 * Calculates the maximal k-cores for a given graph.
 */
public class KShell extends BasicComputation<
    LongWritable, KShellVertexValue, LongWritable, KShellMessage> {

  /** 
  * Logger (levels should be set in log4j.properies)
  */
  private static final Logger logger;
  static { 
    logger = Logger.getLogger(KShell.class);
    logger.setLevel(Level.OFF);
  }
  
  @Override
  public void compute(
      Vertex<LongWritable, KShellVertexValue, LongWritable> vertex,
      Iterable<KShellMessage> messages)
    throws IOException {

    if (0 == getSuperstep()) {

      /** 
      * Set coreness to local degree.
      */
      KShellVertexValue value = new KShellVertexValue(vertex.getNumEdges());
      vertex.setValue(value);

      /**
      * Each node u starts by broadcasting a message(u, du(u)) containing
      * its identifier and degree to all neighbors.
      */
      for (Edge<LongWritable, LongWritable> edge : vertex.getEdges()) {
        sendMessage(
          edge.getTargetVertexId(),
          new KShellMessage(
            vertex.getId().get(), vertex.getValue().get_core()
          )
        );
      }

    } else {

      if (1 == getSuperstep()) {

        /**
        * First time that a vertex receives neighboring node's cores.
        * Build the vertice's hashmap.
        */
        for (KShellMessage message:messages) {
          vertex.getValue().set_neighbor_estimate(
            message.get_neighbor_id(), Integer.MAX_VALUE
          );
        }

      }

      logger.info(vertex.getId() + " got estimates of: ");
      for (KShellMessage message:messages) {
        logger.info(
          message.get_neighbor_id() + " - " + message.get_estimate()
        );
      }

      for (KShellMessage message:messages) {

        /**
        * Whenever u receives a message <v, k> such that k < est[v],
        * the entry est[v] is updated with the new value.
        * A new temporary estimate t is computed by function computeIndex().
        */
        logger.info("Processing message from " + message.get_neighbor_id());
        double temp = vertex.getValue().get_neighbor_estimate(
          message.get_neighbor_id()
        );
        if (message.get_estimate() < temp) {
          vertex.getValue().set_neighbor_new_estimate(
            message.get_neighbor_id(), message.get_estimate()
          );
          double t = vertex.getValue().compute_estimate();
          if (t < vertex.getValue().get_core()) {
            logger.info("Setting new core value! \n\n");
            vertex.getValue().set_core(t);
            vertex.getValue().set_changed(true);
          }
        }
      }
      logger.info("Done recomputing estimate for node " + vertex.getId());
      
      /**
      * One way to terminate the algorithm: Centralized approach.
      * Inform a centralized server whenever no new esimate is generated during
      * a round.
      */
      if (!vertex.getValue().is_changed()) {
        vertex.voteToHalt();
      }

      /**
      * Should be repeated every few time units (round duration)
      */
      if (vertex.getValue().is_changed()) {
        for (Edge<LongWritable, LongWritable> edge : vertex.getEdges()) {
          sendMessage(
            edge.getTargetVertexId(),
            new KShellMessage(
              vertex.getId().get(), vertex.getValue().get_core()
            )
          );
        }
        vertex.getValue().set_changed(false);
      }
    }
  }
}
