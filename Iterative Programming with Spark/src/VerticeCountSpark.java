

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
/*References:
 * https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/JavaSparkPi.java
 */
public final class VerticeCountSpark {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaVerticeCount <file>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("JavaVerticeCount");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    JavaRDD<String> lines = ctx.textFile(args[0], 1);

    JavaRDD<String> vertices = lines.flatMap(new FlatMapFunction<String, String>() {
        @Override
        public Iterable<String> call(String s) {
        	return Arrays.asList(SPACE.split(s));
        }
      });
    JavaRDD<String> uniqueVertices = vertices.distinct();
    JavaRDD<String> uniqueEdges = lines.distinct();
    
    
    
    System.out.println("Num of unique Vertices: " + uniqueVertices.count());
    System.out.println("Num of unique Edges: " + uniqueEdges.count());
    ctx.stop();
  }
}