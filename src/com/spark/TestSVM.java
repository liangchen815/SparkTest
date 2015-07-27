package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

public class TestSVM {

    /**
     * @param args
     */
    public static void main(String[] args) {
	// TODO Auto-generated method stub
	SparkConf conf= new SparkConf().setMaster("local").setAppName("TestSVM");
	SparkContext sc=new SparkContext(conf);
	String path="C:/Users/zhengliang.wu/Desktop/2D_MC_TEST2.txt";
	JavaRDD<LabeledPoint> data=MLUtils.loadLibSVMFile(sc, path).toJavaRDD();
	JavaRDD<LabeledPoint> training=data.sample(false, 0.001);
	System.out.println("测试集："+training.collect());
	training.cache();
	JavaRDD<LabeledPoint> test=data.subtract(training);
	int numIterations=100;
	final SVMModel model=SVMWithSGD.train(training.rdd(), numIterations);//支持向量机只支持二元分类
	model.clearThreshold();
	JavaRDD<Tuple2<Object, Object>> scoreAndLabels=test.map(new Function<LabeledPoint, Tuple2<Object, Object>>() {

	    @Override
	    public Tuple2<Object, Object> call(LabeledPoint p)
		    throws Exception {
		// TODO Auto-generated method stub
		Double score=model.predict(p.features());
		System.out.println("预测值：真实值-----"+score+":"+p.label());
		return new Tuple2<Object, Object>(score, p.label());
	    }
	});
    }

}
