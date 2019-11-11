package aa;
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
    public static enum ReachCounter{ COUNT };
    static enum count_x {
         count_i;
     }
       
	 public static class TokenizerMapper
     extends Mapper<Object, Text, Text, ObjectWritable>{
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
                 context.getCounter(count_x.count_i).increment(1);
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
                context.getCounter(count_x.count_i).increment(1);
            }
            int next_hop = Integer.parseInt(next_h);
             
             next.set(next_hop);
         }
         if(itr.hasMoreTokens())
         {
             itr.nextToken();
         }


         context.write(id,new ObjectWritable(next));
       
             }
             
            }
    public void cleanup(Context context) throws IOException, InterruptedException 
    {
    Iterator iterator = count_ini.entrySet().iterator();
    while (iterator.hasNext()) {
       Map.Entry me2 = (Map.Entry) iterator.next();
       context.write(new Text(me2.getKey().toString()),new ObjectWritable(String.valueOf(context.getCounter(count_x.count_i).getValue())));

    if((int)me2.getValue()==-1)
    {
        IntWritable iw= new IntWritable((int)me2.getValue());
       context.write(new Text(me2.getKey().toString()),new ObjectWritable(iw));
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
                
                int sum = 0;
                arr.clear();
                int ttn=0;
                for (ObjectWritable val : values) 
                {
                
                    //non dangling nodes
                if(val.get() instanceof IntWritable)
                    {
                        int z=((IntWritable)(val.get())).get();
                        if(z!=-1)
                         arr.add(z);
                    }
                else{
                       ttn= Integer.parseInt((String)val.get());
                    }
                }
                
                PRNodeWritable pWritable = new PRNodeWritable(key.toString(),arr,-1,ttn,0.0);
                context.write(key,pWritable);
             }
     
}
     


}
