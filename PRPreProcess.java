import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PRPreProcess {
	 public static class TokenizerMapper
     extends Mapper<Object, Text, Text, IntWritable>{
		 int i=0;
     private HashMap<String, Integer> count_ini = new HashMap<String, Integer>();;
     private Text id = new Text();
     private IntWritable next = new IntWritable();
     
     public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
         Configuration conf = context.getConfiguration();
         StringTokenizer itr = new StringTokenizer(value.toString()," ");
         while (itr.hasMoreTokens()) {
             String idx = itr.nextToken();
         id.set(idx);
         if(itr.hasMoreTokens())
         {
             int next_hop = Integer.parseInt(itr.nextToken());
             next.set(next_hop);
         }
         if(itr.hasMoreTokens())
         {
             itr.nextToken();
         }


         context.write(id,next);
       
             }
             
            }
  
}

public static class IntSumReducer
     extends Reducer<Text,IntWritable,Text,PRNodeWritable> {
     private ArrayList<Integer> arr = new ArrayList<>();
     
     public void reduce(Text key, Iterable<IntWritable> values,
             Context context
             ) throws IOException, InterruptedException {
                
                int sum = 0;
                arr.clear();
                for (IntWritable val : values) 
                {
                   arr.add(val.get());
                }

                PRNodeWritable pWritable = new PRNodeWritable(key.toString(),arr,0.0);
                context.write(key,pWritable);
             }
     
}

public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 conf.set("N",args[2]);
 conf.set("mapreduce.textoutputformat.separator"," ");
 
 Job job = Job.getInstance(conf, "word count");
 job.setJarByClass(PRPreProcess.class);
 job.setMapperClass(TokenizerMapper.class);
 job.setReducerClass(IntSumReducer.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(IntWritable.class);
 FileInputFormat.addInputPath(job, new Path(args[0]));
 FileOutputFormat.setOutputPath(job, new Path(args[1]));
 System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
