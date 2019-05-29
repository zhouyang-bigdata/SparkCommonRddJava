/**
 * @ClassName Filter
 * @Description TODO
 * @Author zy
 * @Date 2019/5/27 21:02
 * @Version 1.0
 **/
package com.app.main.Transformationt;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.List;

/*
 * @Author zhouyang
 * @Description //TODO 
 * @Date 21:05 2019/5/27
 * @Param 
 * @return  
 **/
public class Filter {
    public static void main(String[] args){
        SparkConf conf = new SparkConf();
        conf.setAppName("Filter");
        conf.setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("F:\\sparktest\\sample.txt");
        JavaRDD<String> zksRDD = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("zks");
            }
        });
        //打印内容
        List<String> zksCollect = zksRDD.collect();
        for (String str:zksCollect) {
            System.out.println(str);
        }

//        ----------------输出-------------------
//        ff aa bb zks
//        ee  zz zks

    }
}
