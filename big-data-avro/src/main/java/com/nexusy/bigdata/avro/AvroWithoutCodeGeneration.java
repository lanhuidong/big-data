package com.nexusy.bigdata.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * @author lanhuidong
 * @since 2015-11-14
 */
public class AvroWithoutCodeGeneration {

    public static void main(String[] args) throws IOException {

        Schema schema = new Schema.Parser().parse(AvroWithoutCodeGeneration.class.getClassLoader()
                .getResourceAsStream("user.avsc"));

        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "MJ");
        user1.put("favorite_number", 23);

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "KB");
        user2.put("favorite_number", 24);
        user2.put("favorite_color", "purple");

        File file = new File("users.avro");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.close();

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }
}
