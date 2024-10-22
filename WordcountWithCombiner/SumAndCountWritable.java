
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class SumAndCountWritable implements Writable {
    
    private float sum = 0;
    private int count = 0;

    public int getCount() {
        return count;
    }
    public void setCount(int count) {
        this.count = count;
    }

    public float getSum() {
        return sum;
    }
    public void setSum(float sum) {
        this.sum = sum;
    }
    

    /*
     * serialize the fields of this object to out
     */
    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeInt(count);
        out.writeFloat(sum);
    }

        /*
     * serialize the fields of this object to out
     */
    @Override
    public void read(DataInput in) throws IOException
    {
        sum = in.readFloat();
        count = in.readInt();
    }


    public String toString()
    {
        return "sum="+sum+",count="+count ;
    }


}
