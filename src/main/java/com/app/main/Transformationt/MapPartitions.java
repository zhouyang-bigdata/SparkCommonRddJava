/**
 * @ClassName MapPartitions
 * @Description TODO
 * @Author zy
 * @Date 2019/5/27 21:11
 * @Version 1.0
 **/
package com.app.main.Transformationt;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/*
 * @Author zhouyang
 * @Description //TODO
 * @Date 21:11 2019/5/27
 * @Param
 * @return
 **/
public class MapPartitions {
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setAppName("MapPartitions");
        conf.setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = jsc.parallelize(
                Arrays.asList(1,2,3,4,5,6,7,8,9,10));
        JavaRDD<Integer> mapPartitionRDD = rdd.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Integer> it) throws Exception {
                ArrayList<Integer> results = new ArrayList<>();
                while (it.hasNext()) {
                    int i = it.next();
                    results.add(i*i);
                }
                return results.iterator();
            }
        });
        mapPartitionRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
//        ----------输出-------------
//        1
//        4
//        9
//        16
//        25
//        36
//        49
//        64
//        81
//        100

    }
}
