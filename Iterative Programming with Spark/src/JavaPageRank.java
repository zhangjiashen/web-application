import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;
import java.util.regex.Pattern;

/* References:
 * https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/JavaPageRank.java
 */
public final class JavaPageRank {
	private static final Pattern SPACE = Pattern.compile(" ");
	public static final int num_nodes = 2546953;

	private static class Sum implements Function2<Double, Double, Double> {
		@Override
		public Double call(Double a, Double b) {
			return a + b;
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: JavaPageRank <file> <output>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("PageRank");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);


		JavaRDD<String> lines = ctx.textFile(args[0], 1);

		// all users
		JavaRDD<String> vertices = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(SPACE.split(s));
			}
		});
		// unique users
		JavaRDD<String> uniqueVertices = vertices.distinct();

		// users that are not dangling
		JavaRDD<String> nonDanglingVertices = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s) {
				String follower = SPACE.split(s)[0];
				return Arrays.asList(follower);
			}
		}).distinct();

		JavaRDD<String> popularVertices = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s) {
				String followee = SPACE.split(s)[1];
				return Arrays.asList(followee);
			}
		}).distinct();
		// follows no one
		JavaRDD<String> danglingVertices = uniqueVertices.subtract(nonDanglingVertices);
		// no one follows
		JavaRDD<String> nonPopularVertices = uniqueVertices.subtract(popularVertices);


		JavaPairRDD<String, Iterable<String>> tmpLinks = lines.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) {
				String[] parts = SPACE.split(s);
				return new Tuple2<String, String>(parts[0], parts[1]);
			}
		}).distinct().groupByKey();


		JavaPairRDD<String, Double> ranks = uniqueVertices.mapToPair(new PairFunction<String, String, Double>() {
			@Override
			public Tuple2<String, Double> call(String s) {
				return new Tuple2<String, Double>(s, 1d);
			}
		});
		// final int loopNum = Integer.parseInt(args[2]);

		JavaPairRDD<String, Double> tmpRanks = danglingVertices.mapToPair(new PairFunction<String, String, Double>() {
			@Override
			public Tuple2<String, Double> call(String s) {
				return new Tuple2<String, Double>(s, 1d);
			}
		});

		// Calculates and updates URL ranks continuously using PageRank
		// algorithm.
		for (int iter = 0; iter < 10; iter++) {
			System.out.println("Iteration: " + iter);

			// compute dangling ranks
			System.out.println("******** compute dangling ranks *********");
			JavaRDD<Tuple2<Double, Double>> danglingRanks = ranks.join(tmpRanks).values();

			Tuple2<Double, Double> total = danglingRanks
					.reduce(new Function2<Tuple2<Double, Double>, Tuple2<Double, Double>, Tuple2<Double, Double>>() {

						@Override
						public Tuple2<Double, Double> call(Tuple2<Double, Double> a, Tuple2<Double, Double> b) {
							return new Tuple2<Double, Double>(a._1() + b._1(), null);
						}

					});
			double totalDanglingRanks = total._1();
			final double danglingScore = totalDanglingRanks / num_nodes;

			System.out.println("******** compute contribs *********");
			JavaPairRDD<String, Double> contribs = tmpLinks.join(ranks).values()
					.flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
						@Override
						public Iterable<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {
							int count = Iterables.size(s._1());
							List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
							for (String n : s._1()) {
								results.add(new Tuple2<String, Double>(n, s._2() / count));
							}

							return results;
						}
					});

			System.out.println("******** compute unpopular ranks *********");
			JavaPairRDD<String, Double> nonPopularRanks = nonPopularVertices
					.mapToPair(new PairFunction<String, String, Double>() {
						@Override
						public Tuple2<String, Double> call(String s) {
							return new Tuple2<String, Double>(s, 0.15 + danglingScore * 0.85);
						}
					});

			System.out.println("******** update ranks *********");
			ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
				@Override
				public Double call(Double sum) {
					return 0.15 + (sum + danglingScore) * 0.85;
				}
			}).union(nonPopularRanks);
		}

		ranks.saveAsTextFile(args[1]);

		ctx.stop();
	}
}