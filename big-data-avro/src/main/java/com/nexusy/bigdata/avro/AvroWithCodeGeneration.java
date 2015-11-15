package com.nexusy.bigdata.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * @author lanhuidong
 * @since 2015-11-13
 */
public class AvroWithCodeGeneration {

    public static void main(String[] args) throws IOException {
        User user1 = new User();
        user1.setName("MJ");
        user1.setFavoriteNumber(23);

        User user2 = new User("KB", 24, "purple");

        //builder的性能比直接使用构造函数差, 因为builder会复制数据结构并验证数据
        User user3 = User.newBuilder()
                .setName("WD")
                .setFavoriteNumber(null)  //user1的favoriteColor可以不设置,但是使用builder即使允许null这句也不能少
                .setFavoriteColor("black")
                .build();
        DatumWriter<User> datumWriter = new SpecificDatumWriter<>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(User.getClassSchema(), new File("users.avro"));
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();

        DatumReader<User> datumReader = new SpecificDatumReader<>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<>(new File("users.avro"), datumReader);
        User user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }

    }
}
