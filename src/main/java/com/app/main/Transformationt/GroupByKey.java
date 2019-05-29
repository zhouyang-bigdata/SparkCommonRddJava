/**
 * @ClassName GroupByKey
 * @Description TODO
 * @Author zy
 * @Date 2019/5/27 21:09
 * @Version 1.0
 **/
package com.app.main.Transformationt;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;

/*
 * @Author zhouyang
 * @Description //TODO
 * @Date 21:09 2019/5/27
 * @Param
 * @return
 **/
public class GroupByKey {
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setAppName("GroupByKey");
        conf.setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("F:\\sparktest\\sample.txt");
        JavaRDD<Tuple2<String,Float>> scoreDetails = jsc.parallelize(Arrays.asList(new Tuple2("xiaoming", 75)
                , new Tuple2("xiaoming", 90)
                , new Tuple2("lihua", 95)
                , new Tuple2("lihua", 188)));
        //将JavaRDD<Tuple2<String,Float>> 类型转换为 JavaPairRDD<String, Float>
        JavaPairRDD<String, Float> scoreMapRDD = JavaPairRDD.fromJavaRDD(scoreDetails);

        java.util.Map<String, Iterable<Float>> resultMap = scoreMapRDD.groupByKey().collectAsMap();
        for (String key:resultMap.keySet()) {
            System.out.println("("+key+", "+resultMap.get(key)+")");
        }

    }
}
