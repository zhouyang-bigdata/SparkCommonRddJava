/**
 * @ClassName Join
 * @Description TODO
 * @Author zy
 * @Date 2019/5/27 21:10
 * @Version 1.0
 **/
package com.app.main.Transformationt;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/*
 * @Author zhouyang
 * @Description //TODO
 * @Date 21:10 2019/5/27
 * @Param
 * @return
 **/
public class Join {
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setAppName("FlatMap");
        conf.setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("F:\\sparktest\\sample.txt");
    }
}
