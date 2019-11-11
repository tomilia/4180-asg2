package aa;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.io.Writable;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
public class PRNodeWritable implements Writable {
    public String NodeID;
    private ArrayList<Integer> list;
    private double prv; 
    public int ttn;
    public double dif;
    public PRNodeWritable(){
        list = new ArrayList();
        }
    public PRNodeWritable(String fx, ArrayList<Integer> fy, double fz,int ttn,double dif) {
     this.NodeID = fx;
     this.list = fy;
     this.prv = fz;
     this.ttn=ttn;
     this.dif=dif;
    }
    
    public void setPRV(double pagerank)
    {
        this.prv = pagerank;
    }
    public double getPRV()
    {
        return this.prv;
    }
    public void setDif(double dif){
        this.dif=dif;
    }
    public void setID(String nid)
    {
        this.NodeID = nid;
    }
    public int getttn()
    {
        return ttn;
    }
    public double getDif(){
        return dif;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
     NodeID = in.readUTF();
     int size = in.readInt(); // read ArrayList size
     list.clear();
     for(int i=0;i<size;i++) {
         list.add(in.readInt());
     }	
     prv = in.readDouble();
     ttn = in.readInt();
     dif = in.readDouble();
    }
    @Override
    public void write(DataOutput out) throws IOException {
     out.writeUTF(NodeID);
     out.writeInt(list.size());
     for(int data: list) {
	    out.writeInt(data);
	}	
     out.writeDouble(prv);
     out.writeInt(ttn);
     out.writeDouble(dif);
    }
    @Override
    public String toString() {
        String x="(";
 
        
        for(int data: list) {
            x+=String.valueOf(data)+" ";
        }	
        x+=")";
        x+=" "+String.valueOf(prv)+" "+String.valueOf(ttn)+" "+String.valueOf(dif);
        return x;
    }
   }