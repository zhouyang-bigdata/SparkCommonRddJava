/**
 * @ClassName FlatMap
 * @Description TODO
 * @Author zy
 * @Date 2019/5/27 21:03
 * @Version 1.0
 **/
package com.app.main.Transformationt;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;

/*
 * @Author zhouyang
 * @Description //TODO
 * @Date 21:08 2019/5/27
 * @Param
 * @return
 **/
public class FlatMap {
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setAppName("FlatMap");
        conf.setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("F:\\sparktest\\sample.txt");
        JavaRDD<String> flatMapRDD = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split("\\s+");
                return Arrays.asList(split).iterator();
            }
        });
        //输出第一个
        System.out.println(flatMapRDD.first());
//        ------------输出----------
//        aa

    }
}
