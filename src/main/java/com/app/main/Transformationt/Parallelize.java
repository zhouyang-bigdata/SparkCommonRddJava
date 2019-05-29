/**
 * @ClassName Parallelize
 * @Description TODO
 * @Author zy
 * @Date 2019/5/27 21:39
 * @Version 1.0
 **/
package com.app.main.Transformationt;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/*
 * @Author zhouyang
 * @Description //TODO
 * @Date 21:39 2019/5/27
 * @Param
 * @return
 **/
public class Parallelize {
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setAppName("Parallelize");
        conf.setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> javaStringRDD = jsc.parallelize(Arrays.asList("shenzhen", "is a beautiful city"));
    }
}
