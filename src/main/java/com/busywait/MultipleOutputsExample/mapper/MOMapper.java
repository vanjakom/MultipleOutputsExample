package com.busywait.MultipleOutputsExample.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Vanja Komadinovic
 * @author vanjakom@gmail.com
 */
public class MOMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //TODO
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //TODO
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //TODO
    }
}
