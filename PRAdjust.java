package aa;
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


public class PRAdjust {
   
	 public static class AdjustMapper
     extends Mapper<Object, Text, Text, Text>{
        private HashMap<String, Integer> count_ini = new HashMap<String, Integer>();;
		 int i=0;
     private Text id = new Text();
     private IntWritable next = new IntWritable();
     @Override
     public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
         Configuration conf = context.getConfiguration();
         
             String val[] = value.toString().split("\\D+");
             if(val.length>1)
             {
               String continousNode = "";
             for(int k=1;k<val.length;k++)
             continousNode+=val[k]+" ";
              context.write(new Text(val[0]),new Text(continousNode));
             }
            else
            context.write(new Text(val[0]),new Text("()"));
           
           
             
            }
  
}

public static class AdjustReducer
     extends Reducer<Text,Text,Text,Text> {
     private ArrayList<Integer> arr = new ArrayList<>();
     @Override
     public void reduce(Text key, Iterable<Text> values,
             Context context
             ) throws IOException, InterruptedException {
                for(Text val:values)
               context.write(key,val);
             }
     
}

}
