package com.busywait.MultipleOutputsExample.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Vanja Komadinovic
 * @author vanjakom@gmail.com
 */
public class MOReducerNative extends Reducer<Text, Text, Text, Text> {
    protected MultipleOutputs multipleOutputs = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()) {
            Text next = iterator.next();
            count += Long.parseLong(next.toString());
        }

        context.write(key, new Text("" + count));

        // write to reducerMO MultipleOutput
        multipleOutputs.write("mo", null, key);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}