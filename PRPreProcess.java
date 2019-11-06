package aa;
import aa.PRAdjust.AdjustMapper;
import aa.PRAdjust.AdjustReducer;
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
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob; 
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
        private HashMap<String, Integer> count_ini = new HashMap<String, Integer>();;
		 int i=0;
     private Text id = new Text();
     private IntWritable next = new IntWritable();
     
     public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
         Configuration conf = context.getConfiguration();
         StringTokenizer itr = new StringTokenizer(value.toString()," ");
         while (itr.hasMoreTokens()) {
             String idx = itr.nextToken();
             if(!count_ini.containsKey(idx))
             {
                 count_ini.put(idx, 1);
             }
             else
             {
                 int sum=count_ini.get(idx)+1;
                 count_ini.put(idx, sum);
             }

            Integer.parseInt(idx);
         id.set(idx);
         if(itr.hasMoreTokens())
         {
            String next_h = itr.nextToken();
            if(!count_ini.containsKey(next_h))
            {
                count_ini.put(next_h, -1);
            }
            int next_hop = Integer.parseInt(next_h);
             
             next.set(next_hop);
         }
         if(itr.hasMoreTokens())
         {
             itr.nextToken();
         }


         context.write(id,next);
       
             }
             
            }
    public void cleanup(Context context) throws IOException, InterruptedException 
    {
    Iterator iterator = count_ini.entrySet().iterator();
    while (iterator.hasNext()) {
       Map.Entry me2 = (Map.Entry) iterator.next();
    if((int)me2.getValue()==-1)
    {
       context.write(new Text(me2.getKey().toString()),new IntWritable((int)me2.getValue()));
    }
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
                    //non dangling nodes
                   if(val.get()!=-1)
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
 
 Job job = new Job(conf, "word count");
 job.setJarByClass(PRPreProcess.class);
 job.setMapperClass(TokenizerMapper.class);
 job.setReducerClass(IntSumReducer.class);
 job.setMapOutputKeyClass(Text.class);
 job.setMapOutputValueClass(IntWritable.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(PRNodeWritable.class);
 FileInputFormat.addInputPath(job, new Path(args[0]));

 ControlledJob ctrjob1 = new ControlledJob(conf);
 ctrjob1.setJob(job);
 FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
 JobControl jobs = new JobControl("mycontrol");
 jobs.addJob(ctrjob1);
 
 
 /*
    JOB222
    Job job2 = new Job(conf, "adjust");
job2.setJarByClass(PRPreProcess.class);
job2.setMapperClass(TokenizerMapper.class);
job2.setReducerClass(IntSumReducer.class);
job2.setMapOutputKeyClass(Text.class);
job2.setMapOutputValueClass(IntWritable.class);
job2.setOutputKeyClass(Text.class);
job2.setOutputValueClass(PRNodeWritable.class);
FileInputFormat.addInputPath(job2, new Path(args[0]));
 */

Job job2 = new Job(conf, "adjust");
job2.setJarByClass(PRAdjust.class);
job2.setMapperClass(AdjustMapper.class);
job2.setReducerClass(AdjustReducer.class);
job2.setMapOutputKeyClass(Text.class);
job2.setMapOutputValueClass(Text.class);
job2.setOutputKeyClass(Text.class);
job2.setOutputValueClass(IntWritable.class);
FileInputFormat.addInputPath(job2, new Path(args[1]+"/part*")); 
ControlledJob ctrjob2 = new ControlledJob(conf);
 ctrjob2.setJob(job2);
 ctrjob2.addDependingJob(ctrjob1);
 FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/out2"));  
 jobs.addJob(ctrjob2);
 

 Thread  t=new Thread(jobs);  
 t.start();  
   
 while(true){  
       
     if(jobs.allFinished()){
         
         jobs.stop();  
         System.exit(1);  
         break;  
     }  
       
     if(jobs.getFailedJobList().size()>0){
         jobs.stop();  
         System.exit(1);  
         break;  
     }  
       
 }
 
}
}
