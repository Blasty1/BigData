package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.stream.Stream;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData extends Reducer<
                IntWritable,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
    @Override
    protected void reduce(
        IntWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
        int i=0;
        for(Text value: values)
        {
            i++;
        }
		context.write(new Text("Group" + key.get()),new IntWritable(i));
    }
}
