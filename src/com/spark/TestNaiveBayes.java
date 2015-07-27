package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

public class TestNaiveBayes {

    /**
     * @param args
     */
    public static void main(String[] args) {
	// TODO Auto-generated method stub
	SparkConf conf=new SparkConf().setMaster("local").setAppName("NaiveBayes");
	JavaSparkContext sc=new JavaSparkContext(conf);
	String datapath="C:/Users/zhengliang.wu/Desktop/2D_MC_TEST2.txt";
	JavaRDD<LabeledPoint> data=MLUtils.loadLibSVMFile(sc.sc(), datapath).toJavaRDD().cache();//cache重新计算时无需加载
	JavaRDD<LabeledPoint> training=data.sample(false, 0.7);//随机数种子默认当前时间，所以每次运行产生的随机数都不同
	JavaRDD<LabeledPoint> test=data.subtract(training);
	final NaiveBayesModel model=NaiveBayes.train(training.rdd(),1.0);
	JavaPairRDD<Double, Double> predictionAndLabel=test.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {

	    @Override
	    public Tuple2<Double, Double> call(LabeledPoint p)
		    throws Exception {
		// TODO Auto-generated method stub
		return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
	    }
	});//预测值和label
	JavaPairRDD<Double, Double> filterData=predictionAndLabel.filter(new Function<Tuple2<Double,Double>, Boolean>() {
	    
	    @Override
	    public Boolean call(Tuple2<Double, Double> p1) throws Exception {
		// TODO Auto-generated method stub
		return !p1._1().equals(p1._2);
	    }
	});//过滤出预测值和label不同的点
	System.out.println("Error Data(预测值，实际值):"+filterData.collect());
	Double trainErr=1.0*filterData.count()/test.count();
	System.out.println("Training error:"+trainErr);
	System.out.println("Learned classfication tree model:\n"+model);
    }

}
