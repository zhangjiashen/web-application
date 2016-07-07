

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
/*References:
 * https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/JavaSparkPi.java
 */
public final class FollowerCountSpark {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      System.err.println("Usage: JavaVerticeCount <file> <output>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("JavaFollowerCount");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    JavaRDD<String> lines = ctx.textFile(args[0], 1);
    JavaRDD<String> uniqueEdges = lines.distinct();

    JavaPairRDD<String, Integer> ones = uniqueEdges.mapToPair(new PairFunction<String, String, Integer>() {
        @Override
        public Tuple2<String, Integer> call(String s) {
        	
        	String followee = s.split(" ")[1];
        	return new Tuple2<String, Integer>(followee, 1);
        }
      });
    
    JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      });
    
    
    
    List<Tuple2<String, Integer>> output = counts.collect();
    
    PrintWriter writer = new PrintWriter(args[1]);
    int count = 0;
    for (Tuple2<?,?> tuple : output) {
      writer.println(tuple._1() + "\t" + tuple._2());
      count++;
      if (count % 100 == 0){
    	  writer.flush();
      }
    }
    writer.flush();
    writer.close();
    ctx.stop();
  }
}