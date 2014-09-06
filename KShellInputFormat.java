package org.apache.giraph.examples.kshell;
import org.apache.giraph.examples.kshell.KShellVertexValue;
import java.io.IOException;
import java.util.List;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;
import com.google.common.collect.Lists;

/**
* VertexInputFormat for the KShe;l algorithm
* specified in JSON format.
*/
public class KShellInputFormat extends
  TextVertexInputFormat<LongWritable, KShellVertexValue,
    LongWritable> {

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context) {
    return new KShellInputFormatVertexReader();
  }

 /**
  * Vertex Reader use for the KShell Algorithm
  * The files should be in the following JSON format:
  * JSONArray(<vertex id>, <vertex core value>,
  *   JSONArray(JSONArray(<dest vertex id>, <edge tag>), ...)).
  * The following is a valid example:
  * [1,0,[[2,0],[3,1],[4,1]]]
  */
  class KShellInputFormatVertexReader
    extends TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray,
            JSONException> {

    @Override
    protected JSONArray preprocessLine(Text line) throws JSONException {
      return new JSONArray(line.toString());
    }

    @Override
    protected LongWritable getId(JSONArray jsonVertex) throws JSONException,
              IOException {
      return new LongWritable(jsonVertex.getLong(0));
    }

    @Override
    protected KShellVertexValue getValue(JSONArray jsonVertex)
      throws JSONException, IOException {
      /**
      * Currently the core value is unused when initializing a vertex.
      */
      return new KShellVertexValue();
    }

    @Override
    protected Iterable<Edge<LongWritable, LongWritable>>
    getEdges(JSONArray jsonVertex) throws JSONException, IOException {

      JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);

      /* get the edges */
      List<Edge<LongWritable, LongWritable>> edges =
          Lists.newArrayListWithCapacity(jsonEdgeArray.length());

      for (int i = 0; i < jsonEdgeArray.length(); ++i) {
        LongWritable targetId;
        LongWritable tag;
        JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);

        targetId = new LongWritable(jsonEdge.getLong(0));
        tag = new LongWritable((long) jsonEdge.getLong(1));
        edges.add(EdgeFactory.create(targetId, tag));
      }
      return edges;
    }

    @Override
    protected Vertex<LongWritable, KShellVertexValue,
      LongWritable> handleException(Text line, JSONArray jsonVertex,
          JSONException e) {

      throw new IllegalArgumentException(
          "Couldn't get vertex from line " + line, e);
    }
  }
}
