package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

public class SVMClassifier {

    /**
     * @param args
     */
    public static void main(String[] args) {
	// TODO Auto-generated method stub
	SparkConf conf=new SparkConf().setMaster("local").setAppName("SVM Classifier Example");
	SparkContext sc= new SparkContext(conf);
	String path="E:/Hadoop/spark-1.4.0-bin-hadoop2.6/data/mllib/sample_libsvm_data.txt";
	JavaRDD<LabeledPoint> data=MLUtils.loadLibSVMFile(sc, path).toJavaRDD();//加载稀疏数据
	JavaRDD<LabeledPoint> training=data.sample(false, 0.7,11L);//60%训练集，40%测试集，11L随机数生成器种子
	training.cache();//持久化到内存
	JavaRDD<LabeledPoint> test =data.subtract(training);//data-training=test
	int numIterations=100;//迭代次数
	final SVMModel model=SVMWithSGD.train(training.rdd(), numIterations);//训练模型  Stochastic Gradient Descent 随机梯度下降
	//model.clearThreshold();//清除默认阈值  清理阈值后计算出来的是对应label为1的概率值，没有清理阈值计算出来的是预测的label
	JavaRDD<Tuple2<Object, Object>> scoreAndLabels=test.map(new Function<LabeledPoint, Tuple2<Object,Object>>() {

	    @Override
	    public Tuple2<Object, Object> call(LabeledPoint p)
		    throws Exception {
		// TODO Auto-generated method stub
		Double score=model.predict(p.features());//测试集评分
		return new Tuple2<Object, Object>(score, p.label());
	    }
	});//计算测试集分数
	System.out.println(scoreAndLabels.collect());
	BinaryClassificationMetrics metrics=new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));//获取评价指标
	double auROC=metrics.areaUnderROC();
	System.out.println("Area under ROC="+auROC);
    }

}
