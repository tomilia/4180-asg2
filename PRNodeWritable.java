import java.util.ArrayList;
import java.util.HashMap;

public class PRNodeWritable implement Writable {
    public int NodeID;
    private ArrayList<Integer> list;
    private double prv; 

    public PRNodeWritable(){
        list = new ArrayList();
        }
    public PRNodeWritable(int fx, ArrayList<Integer> fy, double fz) {
     this.NodeID = fx;
     this.list = fy;
     this.prv = fz;
    }
    public void readFields(DataInput in) throws IOException {
     NodeID = in.readInt();
     int size = in.readInt(); // read ArrayList size
     list.clear();
     for(int i=0;i<size;i++) {
         list.add(in.readInt());
     }	
     prv = in.readInt();
    }
    public void write(DataOutput out) throws IOException {
     out.writeInt(NodeID);
     out.writeInt(list.size());
     for(int data: list) {
	    out.writeInt(data);
	}	
     out.writeDouble(prv);
    }
    
   }