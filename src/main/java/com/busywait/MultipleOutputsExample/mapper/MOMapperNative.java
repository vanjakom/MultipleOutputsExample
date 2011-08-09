package com.busywait.MultipleOutputsExample.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * @author Vanja Komadinovic
 * @author vanjakom@gmail.com
 */
public class MOMapperNative  extends Mapper<LongWritable, Text, Text, Text> {
    protected MultipleOutputs multipleOutputs = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value, new Text("1"));

        // write to mapperMO MultipleOutput
        multipleOutputs.write("mo", null, new Text(value));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
