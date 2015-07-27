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
     * ����ͳ�Ʒ���--����ͳ��
     */
    public static void main(String[] args) {
	// TODO Auto-generated method stub
	SparkConf conf = new SparkConf().setMaster("spark://127.0.0.1:7077").setAppName("LibSVM");
	JavaSparkContext jsc = new JavaSparkContext(conf);
	JavaRDD<Vector> mat=MLUtils.loadVectors(jsc.sc(), "E:/Hadoop/1.txt").toJavaRDD();
	MultivariateStatisticalSummary summary=Statistics.colStats(mat.rdd());
	System.out.println("����"+summary.count());
	System.out.println("����"+summary.variance());
	System.out.println("����Ԫ�ظ���"+summary.numNonzeros());
	System.out.println("ƽ��ֵ"+summary.mean());
    }

}
