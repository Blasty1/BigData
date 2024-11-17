package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
        int avg = 0;
        int i = 0;

        List<String> valuesCopy = new ArrayList<>();
        for(Text value : values)
        {
           int rating = Integer.parseInt(value.toString().split(",")[1]);
           valuesCopy.add(value.toString());
           avg += rating;
           i++;
        }
        Float avgComputed =  new Float((float) avg/i);
        for(String value : valuesCopy)
        {
            String[] singleValues = value.split(",");
            Float newRating = Integer.parseInt(singleValues[1]) - avgComputed;
            context.write(key, new Text(singleValues[0] + "," + newRating));
        }

    }
}
