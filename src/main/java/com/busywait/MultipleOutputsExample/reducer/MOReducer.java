package com.busywait.MultipleOutputsExample.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Vanja Komadinovic
 * @author vanjakom@gmail.com
 */
public class MOReducer extends Reducer<Text, Text, Text, Text> {
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
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        Iterator<Text> iterator = values.iterator();
        while (iterator.hasNext()) {
            Text next = iterator.next();
            count += Long.parseLong(next.toString());
        }

        context.write(key, new Text("" + count));

        // write to reducerMO MultipleOutput
        writer.write((key.toString() + "\n").getBytes());
    }

    @Override
    protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {
        writer.close();
    }
}
