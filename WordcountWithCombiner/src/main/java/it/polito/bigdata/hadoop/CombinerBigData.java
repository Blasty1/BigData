package it.polito.bigdata.hadoop;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CombinerBigData extends Reducer
<
    Text,          // Input key type
    IntWritable,   // Input value type
    Text,          // Output key type
    IntWritable    // Output key type
>{
    protected void reduce(
        Text key, //input key
        Iterable<IntWritable> values, // input values
        Context context 
    )throws IOException, InterruptedException
    {
        int occurances = 0;

        for(IntWritable value : values)
        {
            occurances += value.get();
        }

        context.write(key, new IntWritable(occurances));
    }
}
