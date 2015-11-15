package com.nexusy.bigdata.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.IOException;

/**
 * @author lanhuidong
 * @since 2015-11-15
 */
public class AvroFileCat {

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            return;
        }
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(args[0]), datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }

}
