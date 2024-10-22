package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.sql.Array;
import java.util.Arrays;
import java.util.stream.Stream;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    IntWritable,         // Output key type
                    Text> {// Output value type
    private int[] values;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String ranges = context.getConfiguration().get("ranges");
        
        String rangesFormatted = ranges.replace("[", "").replace("]", "");
        this.values = Stream.of(rangesFormatted.split(",")).mapToInt(s -> Integer.parseInt(s.trim())).toArray();
    }
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
        int i=0;
        for(i=0; i < values.length; i++)
        {
            Integer valueInteger = Integer.parseInt(value.toString());
            if(valueInteger < values[i])
            {
                context.write(new IntWritable(i), key);
                return;
            }
        }
        context.write(new IntWritable(i), key);
    		
    }
}
