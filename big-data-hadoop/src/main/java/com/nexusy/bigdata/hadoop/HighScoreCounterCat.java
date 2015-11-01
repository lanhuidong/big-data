package com.nexusy.bigdata.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author lanhuidong
 * @since 2015-11-01
 */
public class HighScoreCounterCat extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.printf("Usage: %S <jobId>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        String jobId = args[0];
        Cluster cluster = new Cluster(getConf());
        Job job = cluster.getJob(JobID.forName(jobId));
        if (job == null) {
            System.err.printf("No job with ID %s found.\n", jobId);
            return -1;
        }
        if (!job.isComplete()) {
            System.err.printf("Job %s is not complete.\n", jobId);
            return -1;
        }
        Counters counters = job.getCounters();
        long gt35 = counters.findCounter(MaxScoreMapper.HighScore.GREATER_THEN_35).getValue();
        System.out.println("There are " + gt35 + " records greater than 35.");
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HighScoreCounterCat(), args);
        System.exit(exitCode);
    }
}
