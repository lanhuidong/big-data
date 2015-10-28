package com.nexusy.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;

/**
 * @author lanhuidong
 * @since 2015-10-28
 *
 * 1.后加载的配置文件中的属性可以覆盖前面的值，但是标记为final的属性不能覆盖
 * 2.可以使用${propertyName}的方式引用其他属性
 * 3.系统属性可以覆盖资源文件中${propertyName}，但是没有在资源文件中定义的系统属性是访问不到的
 */
public class ConfigurationDemo {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.addResource("configuration-1.xml");
        conf.addResource("configuration-2.xml");

        System.setProperty("size", "33");
        System.setProperty("age", "77");
        System.out.println(conf.get("color"));  //blue
        System.out.println(conf.get("size"));  //88
        System.out.println(conf.get("weight"));  //heavy
        System.out.println(conf.get("size-weight"));  //33, heavy

        System.out.println(conf.get("age"));  //null
    }

}
