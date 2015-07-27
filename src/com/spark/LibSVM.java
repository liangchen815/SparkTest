package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

public class LibSVM {

    /**
     * @param args
     */
    public static void main(String[] args) {
	// TODO Auto-generated method stub
	SparkConf conf = new SparkConf().setMaster("local").setAppName("LibSVM");
	JavaSparkContext jsc = new JavaSparkContext(conf);
	JavaRDD<LabeledPoint> examples=MLUtils.loadLibSVMFile(jsc.sc(), "E:/Hadoop/spark-1.4.0-bin-hadoop2.6/data/mllib/sample_libsvm_data.txt").toJavaRDD();
	System.out.println(examples.first());
    }

}
