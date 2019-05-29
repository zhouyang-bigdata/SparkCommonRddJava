/**
 * @ClassName MapPartitionsWithIndex
 * @Description TODO
 * @Author zy
 * @Date 2019/5/27 21:11
 * @Version 1.0
 **/
package com.app.main.Transformationt;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

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
public class MapPartitionsWithIndex {
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setAppName("MapPartitionsWithIndex");
        conf.setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);
        JavaRDD<Tuple2<Integer, Integer>> tuple2JavaRDD = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Tuple2<Integer, Integer>>>() {
            @Override
            public Iterator<Tuple2<Integer, Integer>> call(Integer partIndex, Iterator<Integer> it) throws Exception {
                ArrayList<Tuple2<Integer, Integer>> tuple2s = new ArrayList<>();
                while (it.hasNext()) {
                    int next = it.next();
                    tuple2s.add(new Tuple2<>(partIndex, next));
                }
                return tuple2s.iterator();
            }
        }, false);

        tuple2JavaRDD.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> tp2) throws Exception {
                System.out.println(tp2);
            }
        });
//        -------输出-------------
//        (0,1)
//        (0,2)
//        (0,3)
//        (1,4)
//        (1,5)
//        (1,6)
//        (2,7)
//        (2,8)
//        (2,9)
//        (2,10)

    }
}
