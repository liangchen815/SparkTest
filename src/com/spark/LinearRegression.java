package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;

import scala.Tuple2;

public class LinearRegression {

    /**
     * @param args
     */
    public static void main(String[] args) {
	// TODO Auto-generated method stub
	SparkConf conf=new SparkConf().setMaster("local").setAppName("Linear Regression Example");
	JavaSparkContext sc= new JavaSparkContext(conf);
	String path="E:/Hadoop/spark-1.4.0-bin-hadoop2.6/data/mllib/ridge-data/lpsa.data";
	JavaRDD<String> data = sc.textFile(path);
	JavaRDD<LabeledPoint> paesedData=data.map(new Function<String, LabeledPoint>() {

	    @Override
	    public LabeledPoint call(String line) throws Exception {
		// TODO Auto-generated method stub
		String[] parts=line.split(",");
		String[] features=parts[1].split(" ");
		double[] v=new double[features.length];
		for (int i = 0; i < features.length; i++) {
		    v[i]=Double.parseDouble(features[i]);
		}
		return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(v));
	    }
	});
	int numIterations=100;
	final LinearRegressionModel model=LinearRegressionWithSGD.train(JavaRDD.toRDD(paesedData), numIterations);
	JavaRDD<Tuple2<Double, Double>> valuesAndPreds=paesedData.map(new Function<LabeledPoint, Tuple2<Double, Double>>() {

	    @Override
	    public Tuple2<Double, Double> call(LabeledPoint point)
		    throws Exception {
		// TODO Auto-generated method stub
		double prediction=model.predict(point.features());
		return new Tuple2<Double, Double>(prediction, point.label());
	    }
	});
	Double MSE = new JavaDoubleRDD(valuesAndPreds.map(
		new Function<Tuple2<Double, Double>, Object>() {
		    public Object call(Tuple2<Double, Double> pair) {
			System.out.println(Math.pow(pair._1() - pair._2(), 2.0));
			return Math.pow(pair._1() - pair._2(), 2.0);//Math.pow(底数,几次方)
		    }
		}
	).rdd()).mean();//损失量
	System.out.println("training Mean Squared Error = " + MSE);
    }

}
