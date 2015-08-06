package com.nexusy.bigdata.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author lanhuidong
 * @since 2015-08-06
 */
public class MaxScoreReducerTest {

    @Test
    public void returnsMaximunDoubleInValues() throws IOException {
        new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>()
                .withReducer(new MaxScoreReducer())
                .withInput(new Text("迈克尔乔丹"), Arrays.asList(new DoubleWritable(31.2), new DoubleWritable(35.8), new DoubleWritable(41), new DoubleWritable(27.3), new DoubleWritable(32.3), new DoubleWritable(33.5)))
                .withOutput(new Text("迈克尔乔丹"), new DoubleWritable(41))
                .runTest();
    }
}
