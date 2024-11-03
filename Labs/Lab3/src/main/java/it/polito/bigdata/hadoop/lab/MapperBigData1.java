package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	String[] line = value.toString().split(",");
        String pair;
        for(int index=1; index < line.length; index++)
        {
            for(int indexInner=index+1; indexInner < line.length; indexInner++)
            {
                if(line[indexInner].compareTo(line[index]) < 0)
                {
                    pair = line[indexInner] + "," + line[index];
                }else{
                    pair = line[index] + "," + line[indexInner];
                }
                context.write(new Text(pair), new IntWritable(1));
            }

        }
        
    }
}
