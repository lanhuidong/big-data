package com.nexusy.bigdata.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author lanhuidong
 * @since 2015-08-06
 */
public class MaxScoreMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private NBARecordParser parser = new NBARecordParser();

    enum HighScore {
        GREATER_THEN_35
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if (parser.isValidRecord()) {
            context.write(new Text(parser.getName()), new DoubleWritable(parser.getScore()));
            if (parser.getScore() > 35) {
                context.getCounter(HighScore.GREATER_THEN_35).increment(1);
            }

            // dynamic counter
            context.getCounter("Finals_Times", parser.getName()).increment(1);
        }
    }
}
