package com.busywait.MultipleOutputsExample;

import com.busywait.MultipleOutputsExample.mapper.MOMapper;
import com.busywait.MultipleOutputsExample.mapper.MOMapperNative;
import com.busywait.MultipleOutputsExample.reducer.MOReducer;
import com.busywait.MultipleOutputsExample.reducer.MOReducerNative;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.tools.ant.taskdefs.Exit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Vanja Komadinovic
 * @author vanjakom@gmail.com
 */
public class MainClass {
    protected static Logger logger = LoggerFactory.getLogger(MainClass.class);

    public static void main(String[] args) {
        boolean useNative = true;
        String inputPath = null;
        String outputPath = null;

        try {
            if ("native".equals(args[0])) {
                useNative = true;
            } else if ("custom".equals(args[0])) {
                useNative = false;
            }
            inputPath = args[1];
            outputPath = args[2];
        } catch (Exception e) {
            logger.error("Unable to parse all reqired args");
            logUsage();
            return;
        }

        Configuration configuration = new Configuration();

        Job job = null;

        try {
            job = new Job(configuration);
            job.setJarByClass(MainClass.class);
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            if (useNative) {
                logger.info("Using native MultipleOutputs");
                job.setMapperClass(MOMapperNative.class);
                job.setReducerClass(MOReducerNative.class);

                //set MultipleOutputs
                MultipleOutputs.addNamedOutput(job, "mo", TextOutputFormat.class, NullWritable.class, Text.class);
            } else {
                logger.info("Using custom MultipleOutputs");
                job.setMapperClass(MOMapper.class);
                job.setReducerClass(MOReducer.class);
            }

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        } catch (Exception e) {
            logger.error("Unable to initialize job", e);
        }

        try {
            job.waitForCompletion(true);
        } catch (Exception e) {
            logger.error("Unable to wait for job to finish", e);
        }

        logger.info("Finished");
    }

    protected static void logUsage() {
        logger.info("Usage: run.sh native|custom input_path output_path");
    }
}
