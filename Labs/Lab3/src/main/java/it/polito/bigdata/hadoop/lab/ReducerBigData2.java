package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.Comparator;
import java.util.Vector;
import java.util.stream.Stream;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                NullWritable,           // Input key type
                WordCountWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<WordCountWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        TopKVector<WordCountWritable> pairs = new TopKVector<>(100);
		for(WordCountWritable s : values)
        {
            pairs.updateWithNewElement(new WordCountWritable(s));
        }
        
        for(WordCountWritable s : (Vector<WordCountWritable>) pairs.getLocalTopK())
        {
            context.write(new Text(s.getWord()), new IntWritable(s.getCount()));
        }
    	
    }
}
