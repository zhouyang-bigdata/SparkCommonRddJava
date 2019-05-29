/**
 * @ClassName ReduceByKey
 * @Description TODO
 * @Author zy
 * @Date 2019/5/27 21:07
 * @Version 1.0
 **/
package com.app.main.Transformationt;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

/*
 * @Author zhouyang
 * @Description //TODO
 * @Date 21:08 2019/5/27
 * @Param
 * @return
 **/
public class ReduceByKey {
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setAppName("ReduceByKey");
        conf.setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("F:\\sparktest\\sample.txt");

        JavaPairRDD<String, Integer> wordPairRDD = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                ArrayList<Tuple2<String, Integer>> tpLists = new ArrayList<Tuple2<String, Integer>>();
                String[] split = s.split("\\s+");
                for (int i = 0; i <split.length ; i++) {
                    Tuple2 tp = new Tuple2<String,Integer>(split[i], 1);
                    tpLists.add(tp);
                }
                return tpLists.iterator();
            }
        });

        JavaPairRDD<String, Integer> wordCountRDD = wordPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });
        java.util.Map<String, Integer> collectAsMap = wordCountRDD.collectAsMap();
        for (String key:collectAsMap.keySet()) {
            System.out.println("("+key+","+collectAsMap.get(key)+")");
        }
//        ----------输出-------------------------------
//        (kks,1)
//        (ee,6)
//        (bb,2)
//        (zz,1)
//        (ff,1)
//        (cc,1)
//        (zks,2)
//        (dd,2)
//        (aa,5)

    }
}
