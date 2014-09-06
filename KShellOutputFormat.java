package org.apache.giraph.examples.kshell;
import org.apache.giraph.examples.kshell.KShellVertexValue;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;
import java.io.IOException;


public class KShellOutputFormat extends TextVertexOutputFormat
  <LongWritable, KShellVertexValue, LongWritable> {

  @Override
  public TextVertexWriter createVertexWriter(
      TaskAttemptContext context) {
    return new KShellOutputFormatVertexWriter();
  }

  /**
  * Vertex Writer that outputs the following JSON:
  * [vertex id, vertex k-core value]
  * An example of this is:
  * [1,2]  (vertex id of 1 and k-core value of 2)
  */
  private class KShellOutputFormatVertexWriter extends
    TextVertexWriterToEachLine {
    @Override

    public Text convertVertexToLine(
      Vertex<LongWritable, KShellVertexValue, LongWritable> vertex
    ) throws IOException {

      JSONArray jsonVertex = new JSONArray();

      try {
        jsonVertex.put(vertex.getId().get());
        jsonVertex.put(vertex.getValue().get_core());

      } catch (JSONException e) {
        throw new IllegalArgumentException(
          "KShellOutputFormatVertexWriter: Unable to write" + vertex);
      }
      return new Text(jsonVertex.toString());
    }
  }
}
