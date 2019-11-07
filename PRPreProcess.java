package aa;
import aa.PRAdjust.AdjustMapper;
import aa.PRAdjust.AdjustReducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.lang.*;
import java.util.StringTokenizer;
import org.apache.commons.logging.Log;
import java.math.BigDecimal;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class PRPreProcess {
    static enum count_x {
         count_i;
     }
     private static int c;
	 public static class TokenizerMapper
     extends Mapper<Object, Text, Text, ObjectWritable>{

        private HashMap<String, ArrayList<Integer>> count_ini = new HashMap<String, ArrayList<Integer>>();;
		 int i=0;
     private Text id = new Text();
     private IntWritable next = new IntWritable();
     @Override
     public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
         Configuration conf = context.getConfiguration();
         StringTokenizer itr = new StringTokenizer(value.toString()," ");
         while (itr.hasMoreTokens()) {
             String idx = itr.nextToken();
             if(!count_ini.containsKey(idx))
             {
                 ArrayList<Integer> gg = new ArrayList<Integer>(); 
                 count_ini.put(idx, gg);
                context.getCounter(count_x.count_i).increment(1);
             }
             

         if(itr.hasMoreTokens())
         {
            String next_h = itr.nextToken();
            if(!count_ini.containsKey(next_h))
            {
                ArrayList<Integer> zc = new ArrayList<Integer>();
                count_ini.put(next_h,zc);
                context.getCounter(count_x.count_i).increment(1);
            }

            int next_hop = Integer.parseInt(next_h);
             
             count_ini.get(idx).add(next_hop);
         }
         if(itr.hasMoreTokens())
         {
             itr.nextToken();
         }

         
         
       
             }
             
            }
    public void cleanup(Context context) throws IOException, InterruptedException 
    {
    Iterator iterator = count_ini.entrySet().iterator();
    while (iterator.hasNext()) {
       Map.Entry me2 = (Map.Entry) iterator.next();
       double originalPR=1.0/context.getCounter(count_x.count_i).getValue();
       double p=originalPR;
        PRNodeWritable ac= new PRNodeWritable(me2.getKey().toString(),(ArrayList<Integer>)me2.getValue(),originalPR);
       context.write(new Text(me2.getKey().toString()),new ObjectWritable(ac));
       if(((ArrayList<Integer>)me2.getValue()).size()>0)
       {
        p = originalPR/((ArrayList<Integer>)me2.getValue()).size();       

       for(int a:(ArrayList<Integer>)me2.getValue())
       {
           Object obj = String.valueOf(p);
           context.write(new Text(String.valueOf(a)),new ObjectWritable(obj));
       }
       }

    
  }
  }
}

public static class IntSumReducer
     extends Reducer<Text,ObjectWritable,Text,PRNodeWritable> {
         
     private ArrayList<Integer> arr = new ArrayList<>();
     
     public void reduce(Text key, Iterable<ObjectWritable> values,
             Context context
             ) throws IOException, InterruptedException {
                PRNodeWritable M=new PRNodeWritable();
                double sum=0;
                for (ObjectWritable val:values)
                {
                    if(val.get() instanceof PRNodeWritable)
                    M = (PRNodeWritable)val.get();
                    else
                    sum+=Double.parseDouble((String)val.get());
                }
                M.setPRV(sum);
                context.write(key,M);
               /*
                PRNodeWritable pWritable = new PRNodeWritable(key.toString(),arr,context.getCounter(count_x.count_i).getValue());
                */
                
                
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
 job.setMapOutputValueClass(ObjectWritable.class);
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
job2.setOutputValueClass(Text.class);
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
