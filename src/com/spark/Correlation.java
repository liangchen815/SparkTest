package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

import com.sun.org.glassfish.external.statistics.Statistic;

public class Correlation {

    /**
     * @param args
     */
    public static void main(String[] args) {
	// TODO Auto-generated method stub
	SparkConf conf = new SparkConf().setMaster("local").setAppName("LibSVM");
	JavaSparkContext jsc = new JavaSparkContext(conf);
	JavaRDD<Vector> mat=MLUtils.loadVectors(jsc.sc(), "E:/Hadoop/1.txt").toJavaRDD();
	Matrix correlMatrix=Statistics.corr(mat.rdd(),"pearson");
	System.out.println(correlMatrix);
	System.out.println(mat.rdd().first());
	Vector vec=Vectors.dense(1.0,2.0,3.0);
	ChiSqTestResult goodnessOfFitTestResult=Statistics.chiSqTest(vec);
	System.out.println("假设检验"+goodnessOfFitTestResult);
	ChiSqTestResult independenceTestResult = Statistics.chiSqTest(correlMatrix);
	System.out.println("假设检验"+independenceTestResult);
    }

}
