package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.mllib.stat.Statistics;
public class SummaryStatistics {

    /**
     * 基本统计分析--汇总统计
     */
    public static void main(String[] args) {
	// TODO Auto-generated method stub
	SparkConf conf = new SparkConf().setMaster("spark://127.0.0.1:7077").setAppName("LibSVM");
	JavaSparkContext jsc = new JavaSparkContext(conf);
	JavaRDD<Vector> mat=MLUtils.loadVectors(jsc.sc(), "E:/Hadoop/1.txt").toJavaRDD();
	MultivariateStatisticalSummary summary=Statistics.colStats(mat.rdd());
	System.out.println("数量"+summary.count());
	System.out.println("方差"+summary.variance());
	System.out.println("非零元素个数"+summary.numNonzeros());
	System.out.println("平均值"+summary.mean());
    }

}
