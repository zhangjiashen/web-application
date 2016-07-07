import java.io.IOException;
import java.util.ArrayList;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* References:
 * https://piazza.com/class/icpfeo1f6wt2g5?cid=2236
 * https://hadoop.apache.org/docs/r2.4.1/api/
 * http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 * http://halitics.blogspot.com/2013/08/map-reduce-example-number-of-words-per.html
 * https://hadooptutorial.wikispaces.com/Custom+partitioner
 * */
 
public class AutoCompleteCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String line = value.toString();
    	line = line.replaceAll("[^a-zA-Z]", " ");
    	line = line.toLowerCase(Locale.ENGLISH);
    	int i;
    	String[] words =  line.split("[^a-zA-Z]");
    	for (String word : words){
    		if (word != null && word.length() > 0){
    			for (i=1; i <= word.length(); i++){
    				context.write(new Text(word.substring(0, i)), one);
    			}
    		}
    	}
    	
    }
    
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
    	  sum += val.get();
      }
      if (sum > 2){
    	  result.set(sum);
    	  context.write(key, result);
      }
    }
  }

  

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "AutoComplete count");
    job.setJarByClass(AutoCompleteCount.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    //job.setPartitionerClass(GramPartitioner.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    //job.setNumReduceTasks(5);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}