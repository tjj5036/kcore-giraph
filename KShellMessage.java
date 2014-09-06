package org.apache.giraph.examples.kshell;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

public class KShellMessage implements Writable, Configurable {

  private Configuration conf;
  private long node;
  private double estimate;
  
  public KShellMessage() {}

  /**
   * @param node  Identifier of the sender node
   * @param estimate Index estimate
   */
  public KShellMessage(long node, double estimate) {
    this.node = node;
    this.estimate = estimate;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    node = input.readLong();
    this.estimate = input.readDouble();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeLong(node);
    output.writeDouble(this.estimate);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return(this.conf);
  }

  /**
   * @return long Node Id
   */
  public long get_neighbor_id() {
    return(this.node);
  }

  /**
   * @return double the estimate
   */
  public double get_estimate() {
    return(this.estimate);
  }


  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("{ sender: " +  this.node + "; estimate: ");
    buffer.append(this.estimate + " }");
    return(buffer.toString());
  }
}
