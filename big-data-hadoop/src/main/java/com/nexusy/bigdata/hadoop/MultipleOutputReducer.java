package com.nexusy.bigdata.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * @author lanhuidong
 * @since 2015-11-09
 */
public class MultipleOutputReducer extends Reducer<Text, DoubleWritable, NullWritable, DoubleWritable> {

    private MultipleOutputs<NullWritable, DoubleWritable> multipleOutputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException,
            InterruptedException {
        double maxValue = Double.MIN_VALUE;
        for (DoubleWritable value : values) {
            maxValue = Math.max(value.get(), maxValue);
        }
        multipleOutputs.write(NullWritable.get(), new DoubleWritable(maxValue), key.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
