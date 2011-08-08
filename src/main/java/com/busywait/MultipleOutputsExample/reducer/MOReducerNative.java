package com.busywait.MultipleOutputsExample.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Vanja Komadinovic
 * @author vanjakom@gmail.com
 */
public class MOReducerNative extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //TODO
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //TODO
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //TODO
    }
}