package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    WordCountWritable> {// Output value type

    private TopKVector<WordCountWritable> listOfPairs = new TopKVector<>(100);
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
        
        WordCountWritable pair = new WordCountWritable(key.toString(),Integer.parseInt(value.toString()));

        listOfPairs.updateWithNewElement(pair);

    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        
        for(WordCountWritable w : listOfPairs.getLocalTopK())
        {
            context.write(NullWritable.get(),w);
        }
    }
}
