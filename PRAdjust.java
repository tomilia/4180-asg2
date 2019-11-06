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
     
     public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
         Configuration conf = context.getConfiguration();
         
             String idz[]=value.toString().split(" ",1);
            context.write(new Text(idz[0]),new Text(value.toString()));
           
           
             
            }
  
}

public static class AdjustReducer
     extends Reducer<Text,Text,Text,IntWritable> {
     private ArrayList<Integer> arr = new ArrayList<>();
     
     public void reduce(Text key, Iterable<IntWritable> values,
             Context context
             ) throws IOException, InterruptedException {
                
               context.write(new Text("a"),new IntWritable(1));
             }
     
}

}
