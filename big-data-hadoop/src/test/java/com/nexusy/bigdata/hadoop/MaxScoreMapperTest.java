package com.nexusy.bigdata.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

/**
 * @author lanhuidong
 * @since 2015-08-05
 */
public class MaxScoreMapperTest {

    @Test
    public void processesValidRecord() throws IOException {
        Text value = new Text("迈克尔乔丹 1991 31.2 6.6 11.4 2.8 1.4");
        new MapDriver<LongWritable, Text, Text, DoubleWritable>()
                .withMapper(new MaxScoreMapper())
                .withInput(new LongWritable(0), value)
                .withOutput(new Text("迈克尔乔丹"), new DoubleWritable(31.2))
                .runTest();
    }
}
