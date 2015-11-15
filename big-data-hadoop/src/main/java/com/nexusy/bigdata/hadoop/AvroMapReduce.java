package com.nexusy.bigdata.hadoop;

import com.nexusy.bigdata.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author lanhuidong
 * @since 2015-11-14
 * <p>
 * CMD: hadoop jar big-data-hadoop-bin.jar com.nexusy.bigdata.hadoop.AvroMapReduce users.avro avro
 */
public class AvroMapReduce extends Configured implements Tool {

    public static class AvroDemoMapper extends Mapper<AvroKey<User>, NullWritable, Text, IntWritable> {

        @Override
        protected void map(AvroKey<User> key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            CharSequence color = key.datum().getFavoriteColor();
            if (color == null) {
                color = "none";
            }
            context.write(new Text(color.toString()), new IntWritable(1));
        }
    }

    public static class AvroDemoReducer extends Reducer<Text, IntWritable, AvroKey<CharSequence>, AvroValue<Integer>> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(new AvroKey<>(key.toString()), new AvroValue<>(sum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AvroMapReduce <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        //为了解决jar包冲突导致的错误
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf);
        job.setJarByClass(AvroMapReduce.class);
        job.setJobName("Avro MapReduce");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setMapperClass(AvroDemoMapper.class);
        AvroJob.setInputKeySchema(job, User.getClassSchema());
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setReducerClass(AvroDemoReducer.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroMapReduce(), args);
        System.exit(exitCode);
    }
}
