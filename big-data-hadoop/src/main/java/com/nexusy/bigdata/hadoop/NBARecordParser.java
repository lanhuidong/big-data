package com.nexusy.bigdata.hadoop;

import org.apache.hadoop.io.Text;

/**
 * @author lanhuidong
 * @since 2015-08-06
 */
public class NBARecordParser {

    private String name;
    private String year;
    private double score;
    private double rebounds;
    private double assists;
    private double steals;
    private double blocks;
    private boolean valid;

    public String getName() {
        return name;
    }

    public String getYear() {
        return year;
    }

    public double getScore() {
        return score;
    }

    public double getRebounds() {
        return rebounds;
    }

    public double getAssists() {
        return assists;
    }

    public double getSteals() {
        return steals;
    }

    public double getBlocks() {
        return blocks;
    }

    public void parse(String record) {
        String[] fields = record.split(" ");
        if (fields.length == 7) {
            name = fields[0];
            year = fields[1];
            score = Double.parseDouble(fields[2]);
            rebounds = Double.parseDouble(fields[3]);
            assists = Double.parseDouble(fields[4]);
            steals = Double.parseDouble(fields[5]);
            blocks = Double.parseDouble(fields[6]);
            valid = true;
        }
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public boolean isValidRecord() {
        return valid;
    }
}
