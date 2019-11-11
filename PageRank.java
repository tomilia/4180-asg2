package aa;
import aa.PRAdjust.PRMapper;
import aa.PRAdjust.PRReducer;
import aa.PRPreProcess.TokenizerMapper;
import aa.PRPreProcess.IntSumReducer;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class PageRank {


public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 conf.set("alpha",args[0]);
 conf.set("iteration",args[1]);
 conf.set("mapreduce.textoutputformat.separator"," ");
 int depth=0;
 Job job = new Job(conf, "word count");
 job.setJarByClass(PRPreProcess.class);
 job.setMapperClass(TokenizerMapper.class);
 job.setReducerClass(IntSumReducer.class);
 job.setMapOutputKeyClass(Text.class);
 job.setMapOutputValueClass(ObjectWritable.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(PRNodeWritable.class);
 FileInputFormat.addInputPath(job, new Path(args[2]));

// ControlledJob ctrjob1 = new ControlledJob(conf);
 //ctrjob1.setJob(job);

 FileOutputFormat.setOutputPath(job, new Path("/input/depth0"));
 job.waitForCompletion(true);
 depth++;
 //JobControl jobs = new JobControl("mycontrol");
 //jobs.addJob(ctrjob1);
 
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
/*
job2.setJarByClass(PRAdjust.class);
job2.setMapperClass(PRMapper.class);
job2.setReducerClass(PRReducer.class);
job2.setMapOutputKeyClass(Text.class);
job2.setMapOutputValueClass(ObjectWritable.class);
job2.setOutputKeyClass(Text.class); 

job2.setOutputValueClass(PRNodeWritable.class);
FileInputFormat.addInputPath(job2, new Path(args[1]+"/depth"+(depth-1)+"/part*")); 
 FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/out"+depth));  
 ControlledJob ctrjob2 = new ControlledJob(conf);
 ctrjob2.setJob(job2);
 ctrjob2.addDependingJob(ctrjob1);

    ControlledJob prevjob=ctrjob2;
//will change to iterative
jobs.addJob(ctrjob2);
jobs
    long counter =0;
    .getValue();
    */
    for(int a=0;a<Integer.parseInt(args[1]);a++)
    {
    long counter =0;
   conf = new Configuration();
   // set the depth into the configuration
   conf.set("redepth", depth + "");
    conf.set("alpha",args[0]);
 conf.set("iteration",args[1]);
       job2 = new Job(conf, "adjust"); 
       
  //  ctrjob2 = new ControlledJob(conf);
job2.setJarByClass(PRAdjust.class);
job2.setMapperClass(PRMapper.class);
job2.setReducerClass(PRReducer.class);
job2.setMapOutputKeyClass(Text.class);
job2.setMapOutputValueClass(ObjectWritable.class);
job2.setOutputKeyClass(Text.class);
job2.setOutputValueClass(LongWritable.class);

FileInputFormat.addInputPath(job2, new Path("/input/depth"+(depth-1)+"/part*")); 
/*
ctrjob2.addDependingJob(prevjob);
ctrjob2 = new ControlledJob(conf);
ctrjob2.setJob(job2);
*/
if(a==Integer.parseInt(args[1])-1)
FileOutputFormat.setOutputPath(job2, new Path(args[3]));
else
FileOutputFormat.setOutputPath(job2, new Path("/input/depth"+depth));  
job2.waitForCompletion(true);
depth++;
//jobs.addJob(ctrjob2);
 //counter = job2.getCounters().findCounter(PRAdjust.UpdateCounter.UPDATED).getValue();
}
/*
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
 */
}
}