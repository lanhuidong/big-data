package com.nexusy.bigdata.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author lanhuidong
 * @since 2015-08-06
 */
public class MaxScoreReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double maxValue = Double.MIN_VALUE;
        for (DoubleWritable value : values) {
            maxValue = Math.max(value.get(), maxValue);
        }
        context.write(key, new DoubleWritable(maxValue));
    }
}
