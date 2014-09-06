package org.apache.giraph.examples.kshell;
import org.apache.log4j.Logger;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.lang.Math.*;
import org.apache.hadoop.io.Writable;

/**
* Vertex value used for the Shell implementation 
*/

public class KShellVertexValue implements Writable {

  private final static Logger logger = Logger.getLogger(KShell.class.getName());
  
  /** 
  * Integer array containing one element for reach neighbor
  * Represents the most up to date estimate of the coreness of v known by u.
  * In the absence of more precise information, 
  * all entires are initialized to + infinity
  */
  private HashMap<Long, Double> estimates;
  /** 
  * Core is an integer that represents the local estimate of the 
  * corness of u, initialized with the local degree.
  */
  private double core;
  /**
  * Changed is a boolean flag set to true if core has been recently modified.
  * Initially set to false
  */
  private boolean changed;

  /**
  * Default constructor
  */
  public KShellVertexValue() {
    this.changed = false;
    this.core = 0;
    this.estimates = new HashMap<Long, Double>();
  }

  /**
  * Paramterized constructor
  * 
  * @param core the initial estimate of the vertex
  */
  public KShellVertexValue(double core) {
    this.changed = false;
    this.core = core;
    this.estimates = new HashMap<Long, Double>();
  }

  /**
  * Paramterized constructor
  * 
  * @param core the initial estimate of the vertex
  * @param  estimates the initial estimates of the neighbors
  */
  public KShellVertexValue(double core,
    HashMap<Long, Double> estimates) {
    this.changed = false;
    this.estimates = estimates;
    this.core = core;
  }

  // Serialization functions -----------------------------------------------

  @Override
  public void readFields(DataInput input) throws IOException {
    this.core = input.readDouble();
    this.changed = input.readBoolean();

    int sz = input.readInt();
    for (int i = 0; i < sz; ++i) {
      Long node = input.readLong();
      Double estimate = input.readDouble();
      this.estimates.put(node, estimate);
    }
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeDouble(this.core);
    output.writeBoolean(this.changed);
    int sz = this.estimates.size();
    output.writeInt(sz);

    for (Map.Entry<Long, Double> entry : this.estimates.entrySet()) {
      output.writeLong(entry.getKey());
      output.writeDouble(entry.getValue());
    }
  }

  // Accessors -------------------------------------------------------------
  
  /**
  * @return true if free, false otherwise
  */
  public boolean is_changed() {
    return this.changed;
  }

  /**
  * @param changed the value to set vertex's changed flag to.
  */
  public void set_changed(boolean changed) {
    this.changed = changed;
  }

  /**
  * @return coreness 
  */
  public double get_core() {
    return this.core;
  }

  /**
  * @param core new core value
  */
  public void set_core(double core) {
    this.core = core;
  }

  /** 
  * @param neighbor the neighbor who's core value is needed.
  * @return the estimate for a given neighbor or -1 if not present.
  */
  public double get_neighbor_estimate(long neighbor) {
    if (this.estimates.containsKey(neighbor)) {
      return this.estimates.get(neighbor);
    }
    return(-1);
  }

  /**
  * @param vertex_id id of the current vertex.
  * Logs the estimates of all neighbors.
  */
  public void log_neighbor_estimates(long vertex_id) {
    logger.info(vertex_id + " neighbor estimates: ");
    for (Map.Entry<Long, Double> entry : this.estimates.entrySet()) {
      logger.info("\t" + entry.getKey() +  "-" + entry.getValue());
    }
  }

  /**
  * Sets a new neighbor id to a core value.
  *
  * @param neighbor identifier of the node
  * @param estimate coreness of the node
  */
  public void set_neighbor_estimate(long neighbor, double estimate) {
    this.estimates.put(neighbor, estimate);
  }

  /**
  * Sets a new value in the hashmap.
  * @param neighbor the neighbor who's estimate that needs to change.
  * @param destimate the new estimate for that neighbor.
  */
  public void set_neighbor_new_estimate(long neighbor, double estimate){
    if (this.estimates.containsKey(neighbor)) {
      this.estimates.put(neighbor, estimate);
    }
  }

  /**
  * A new temporary estimate t is compute by function computeIndex().
  * If t is smaller than the previously known value of core, 
  * core is modified and the changed flag is set to true.
  *
  * @return largest value i such that there are at least i entries equal or
  * larger than i in est 
  */
  public double compute_estimate() {
    double old = this.core;
    double[] count = new double[(int)this.core+1];
    
    for (Map.Entry<Long, Double> entry : this.estimates.entrySet()) {
      logger.info("Processing " + entry.getKey() + ": " + entry.getValue());
      double j = Math.min(this.core, entry.getValue().doubleValue());
      logger.info("Min: " + j);
      count[(int)j] = count[(int)j] + 1;
    }
    
    logger.info("Count before");
    int i;
    for (i = 0; i < count.length; i++) {
        logger.info(i + " " + count[i]);
    }
    
    for (i = (int)this.core; i > 1; i--) 
      count[i-1] = count[i-1] + count[i];

    logger.info("Count after");
    for (i = 0; i < count.length; i++) {
        logger.info(i + " " + count[i]);
    }

    i = (int)this.core;
    while ((i > 1) && (count[i] < i)) {
      logger.info("Decrementing" + i + " down one because " + count[i] + " is less than that");
      i = i - 1;  
    }
    logger.info("Loop terminated: i: " + i + " and count[i] = " + count[i]);
    
    if ((double)i != old) {
      logger.info("New Core Estimate: " + i + "\n");
    }
    return (double)i;
  }
}
