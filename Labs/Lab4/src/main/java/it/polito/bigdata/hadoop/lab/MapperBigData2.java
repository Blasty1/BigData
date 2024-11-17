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
class MapperBigData2 extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    private HashMap<String,Float[]> pairsToReturn = new HashMap<>();
   
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] values = value.toString().split(",");
            Float ratingNormalized = Float.parseFloat(values[1]);

            if( pairsToReturn.containsKey(values[0]) )
            {
                Float[] totalAndNumLocal = pairsToReturn.get(values[0]);
                totalAndNumLocal[0] += ratingNormalized;
                totalAndNumLocal[1]++;
                pairsToReturn.put(values[0],totalAndNumLocal);
            }else{
                Float[] totalAndNumLocal = new Float[2];
                totalAndNumLocal[0] = ratingNormalized;
                totalAndNumLocal[1] =  Float.parseFloat("1.0");

                pairsToReturn.put(values[0],totalAndNumLocal);
            }
            
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(String key : pairsToReturn.keySet())
        {
            context.write(new Text(key), new Text(pairsToReturn.get(key)[0] + "," + pairsToReturn.get(key)[1]));
        }
    }
}
