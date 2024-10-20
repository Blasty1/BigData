package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Split each sentence in words. Use whitespace(s) as delimiter 
    		// (=a space, a tab, a line break, or a form feed)
    		// The split method returns an array of strings
            String[] words = value.toString().split("\\s+");
            String oldWord="";

            // Iterate over the set of words
            for(String word : words) {
                if(words[0].equals(word))
                {
                    oldWord = word.toLowerCase();
                    continue;
                }
            	// Transform word case
                String cleanedWord = word.toLowerCase();
                
                // emit the pair (word, 1)
                context.write(new Text(oldWord + " " + cleanedWord),
                		      new IntWritable(1));
            }
    }
}
