package com.busywait.MultipleOutputsExample.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Vanja Komadinovic
 * @author vanjakom@gmail.com
 */
public class MOMapper extends Mapper<LongWritable, Text, Text, Text> {
    FSDataOutputStream writer = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // format: attempt_200811201130_0004_m_000003_0
        String[] taskId = context.getConfiguration().get("mapred.task.id").split("_");

        Path workPath = FileOutputFormat.getWorkOutputPath(context);
        workPath = workPath.suffix("/mo-"+ taskId[3] + "-" + taskId[4].substring(1));
        writer = workPath.getFileSystem(context.getConfiguration()).create(workPath);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value, new Text("1"));

        // write to mapperMO MultipleOutput
        writer.write((value.toString() + "\n").getBytes());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        writer.close();
    }
}
