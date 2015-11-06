package com.nexusy.bigdata.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author lanhuidong
 * @since 2015-11-06
 * <p>
 * CMD: hadoop jar big-data-hadoop.jar com.nexusy.bigdata.hadoop.SmallFilesToSequenceFile "s[a-f].txt" sequenceFile
 * CMD: hadoop fs -text sequenceFile/part-r-00000
 * CMD: hadoop fs -text sequenceFile/part-r-00001
 */
public class SmallFilesToSequenceFile extends Configured implements Tool {

    static class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

        private Text fileNameKey;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            Path file = ((FileSplit) inputSplit).getPath();
            fileNameKey = new Text(file.toString());
        }

        @Override
        protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(fileNameKey, value);
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }

        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setMapperClass(SequenceFileMapper.class);
        job.setNumReduceTasks(2);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SmallFilesToSequenceFile(), args);
        System.exit(exitCode);
    }
}
