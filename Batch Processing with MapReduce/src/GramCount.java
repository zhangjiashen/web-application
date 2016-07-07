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
 
public class GramCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String line = value.toString();
    	ArrayList<String> twoGram = new ArrayList<String>(3);
    	ArrayList<String> threeGram = new ArrayList<String>(4);
    	ArrayList<String> fourGram = new ArrayList<String>(5);
    	ArrayList<String> fiveGram = new ArrayList<String>(6);
    	line = line.replaceAll("[^a-zA-Z]", " ");
    	//System.out.println(line);
    	line = line.toLowerCase(Locale.ENGLISH);
    	//System.out.println(line);
    	String[] words =  line.split("[^a-zA-Z]");
    	for (String word : words){
    		if (word != null && word.length() > 0){
    			// 1-gram
    			context.write(new Text(word), one);
    			// 2-gram
    			writeContext(twoGram, word, 2, context);
    			// 3-gram
    			writeContext(threeGram, word, 3, context);
    			// 4-gram
    			writeContext(fourGram, word, 4, context);
    			// 5-gram
    			writeContext(fiveGram, word, 5, context);
    			
    		}
    	}
    	
    }
    
    public void writeContext(ArrayList<String> list, String word, int len, Context context) throws IOException, InterruptedException{
    	String grams = null;
    	int i;
		list.add(word);
    	if (list.size() > len){
			list.remove(0);
		}
    	if (list.size() == len){
    		grams = new String(list.get(0));
			for (i=1; i < list.size(); i++){
				grams += " " + list.get(i);
			}
    		context.write(new Text(grams), one);
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
  /*
  public static class GramPartitioner
  		extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		int i;
		int count = 0;
		String line = key.toString();
		for (i=0; i < line.length(); i++){
			if (line.charAt(i) == ' '){
				count++;
			}
		}
		return count % numReduceTasks;
	}
	  
  }*/
  

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "gram count");
    job.setJarByClass(GramCount.class);
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