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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class PRAdjust {
    static enum count_x {
         count_i;
     }

     private static int c;
	 public static class PRMapper
     extends Mapper<Object, Text, Text, ObjectWritable>{

        private HashMap<String, ArrayList<Integer>> count_ini = new HashMap<String, ArrayList<Integer>>();;
		 int i=0;
     private Text id = new Text();
     private IntWritable next = new IntWritable();
     @Override
     public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
         Configuration conf = context.getConfiguration();
         String cas[] = value.toString().replaceAll("\\(", "").replaceAll("\\)", "").split("\\s+");
        
        String idx = cas[0];
        String prv = cas[cas.length-3];
        String ttn = cas[cas.length-2];
        PRNodeWritable pnode;
        ArrayList<Integer> c = new ArrayList<Integer>();
        double init = 1.0/Integer.parseInt(ttn);
        if(cas.length>4)
        {
            //rebuild the node
            
            String outnode="";
            
            for(int k=1;k<=cas.length-4;k++)
            {
                c.add(Integer.parseInt(cas[k]));
                outnode+=cas[k]+" ";
            }
            //init
            if(Double.parseDouble(prv) == -1.0)
            {
                prv=String.valueOf(init);
            }
            else{
                prv=String.valueOf(Double.parseDouble(prv));
            }
            double divprv = Double.parseDouble(prv)/c.size();
            pnode = new PRNodeWritable(idx,c,Double.parseDouble(prv),Integer.parseInt(ttn),0.0);
            
            context.write(new Text(cas[0]),new ObjectWritable(pnode));
            
            for(Integer z: c)
            {
                context.write(new Text(String.valueOf(z)),new ObjectWritable(String.valueOf(divprv)));
            }
        }
        //dangling nodes
        else
        {
            /*
            if(Double.parseDouble(prv) == -1.0)
            {
                prv=String.valueOf(init);
            }
            else{
                prv=String.valueOf(Double.parseDouble(prv));
            }
            */
            double divprv = 0;
            pnode = new PRNodeWritable(idx,c,divprv,Integer.parseInt(ttn),0.0);
            context.write(new Text(cas[0]),new ObjectWritable(pnode));
        }
             }
         /*
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
             */
             
            }
         /*   
    public void cleanup(Context context) throws IOException, InterruptedException 
    {
    Iterator iterator = count_ini.entrySet().iterator();
    while (iterator.hasNext()) {
       Map.Entry me2 = (Map.Entry) iterator.next();
       double originalPR=1.0/context.getCounter(count_x.count_i).getValue();
       double p=originalPR;
        PRNodeWritable ac= new PRNodeWritable(me2.getKey().toString(),(ArrayList<Integer>)me2.getValue(),originalPR,0);
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
  */


public static class PRReducer
     extends Reducer<Text,ObjectWritable,Text,PRNodeWritable> {
         public ArrayList<PRNodeWritable> adjusted=new ArrayList<>();
         private int total_node=0;
     private ArrayList<Integer> arr = new ArrayList<>();
     private double new_prmass=0;
     
     public void reduce(Text key, Iterable<ObjectWritable> values,
             Context context
             ) throws IOException, InterruptedException {
                PRNodeWritable M=new PRNodeWritable();
                double sum=0;
                
                for (ObjectWritable val:values)
                {
                    if(val.get() instanceof PRNodeWritable)
                    {
                     M = (PRNodeWritable)val.get();
                     total_node=M.ttn;
                    }
                    else
                    sum+=Double.parseDouble((String)val.get());
                }
                M.setDif(M.getPRV());
                new_prmass +=sum;
                M.setPRV(sum);
                
                
                adjusted.add(M);
                
                
               /*
                PRNodeWritable pWritable = new PRNodeWritable(key.toString(),arr,context.getCounter(count_x.count_i).getValue());
                */
                
}
           public void cleanup(Context context) throws IOException, InterruptedException 
    {
        Configuration conf = context.getConfiguration();
String param = conf.get("alpha");
Double alpha = Double.parseDouble(param);
    for(PRNodeWritable m: adjusted) {

        double newp=alpha*(1.0/m.ttn)+(1-alpha)*((1-new_prmass)/m.ttn+m.getPRV());
        m.setPRV(newp);
        m.setDif(newp-m.getDif());
            context.write(new Text(m.NodeID),m);
        }
        //context.write(new Text("sca"),new LongWritable(context.getCounter(UpdateCounter.UPDATED).getValue()));
    }
    }
     
     
             
    




}
