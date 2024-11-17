package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {


        Float total=new Float(0);
        Float numOfElements = new Float(0);
        for( Text value : values)
        {
            String[] floatNumbersInStringFormat = value.toString().split(",");
            
            total += Float.valueOf(floatNumbersInStringFormat[0]);
            numOfElements += Float.valueOf(floatNumbersInStringFormat[1]);

        }
        Float result = total/numOfElements;
        context.write(key,new FloatWritable(result));
    }
}
