package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashMap;

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
                    Text> {// Output value type

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
            if(key.get() == 0)
            {
                //skip the head line
                return;
            }
            
            String[] valuesOfTheCvFile = value.toString().split(",");
            context.write(new Text(valuesOfTheCvFile[2]), new Text(valuesOfTheCvFile[1] + "," + valuesOfTheCvFile[6]));    		
    }
}
