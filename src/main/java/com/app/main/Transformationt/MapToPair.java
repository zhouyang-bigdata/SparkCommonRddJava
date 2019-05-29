/**
 * @ClassName MapToPair
 * @Description TODO
 * @Author zy
 * @Date 2019/5/27 21:04
 * @Version 1.0
 **/
package com.app.main.Transformationt;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/*
 * @Author zhouyang
 * @Description //TODO
 * @Date 21:05 2019/5/27
 * @Param
 * @return
 **/
public class MapToPair {
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setAppName("MapToPair");
        conf.setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("F:\\sparktest\\sample.txt");

        //输入的是一个string的字符串，输出的是一个(String, Integer) 的map
        JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s.split("\\s+")[0], 1);
            }
        });

    }
}
