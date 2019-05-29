/**
 * @ClassName Map
 * @Description TODO
 * @Author zy
 * @Date 2019/5/27 21:03
 * @Version 1.0
 **/
package com.app.main.Transformationt;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

/*
 * @Author zhouyang
 * @Description //TODO
 * @Date 21:03 2019/5/27
 * @Param
 * @return
 **/
public class Map {
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setAppName("Map");
        conf.setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("F:\\sparktest\\sample.txt");
        JavaRDD<Iterable<String>> mapRDD = lines.map(new Function<String, Iterable<String>>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                String[] split = s.split("\\s+");
                return Arrays.asList(split);
            }
        });
        //读取第一个元素
        System.out.println(mapRDD.first());
//        ---------------输出-------------
//    [aa, bb, cc, aa, aa, aa, dd, dd, ee, ee, ee, ee]

    }
}
