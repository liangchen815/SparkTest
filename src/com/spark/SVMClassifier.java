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
	JavaRDD<LabeledPoint> data=MLUtils.loadLibSVMFile(sc, path).toJavaRDD();//����ϡ������
	JavaRDD<LabeledPoint> training=data.sample(false, 0.7,11L);//60%ѵ������40%���Լ���11L���������������
	training.cache();//�־û����ڴ�
	JavaRDD<LabeledPoint> test =data.subtract(training);//data-training=test
	int numIterations=100;//��������
	final SVMModel model=SVMWithSGD.train(training.rdd(), numIterations);//ѵ��ģ��  Stochastic Gradient Descent ����ݶ��½�
	//model.clearThreshold();//���Ĭ����ֵ  ������ֵ�����������Ƕ�ӦlabelΪ1�ĸ���ֵ��û��������ֵ�����������Ԥ���label
	JavaRDD<Tuple2<Object, Object>> scoreAndLabels=test.map(new Function<LabeledPoint, Tuple2<Object,Object>>() {

	    @Override
	    public Tuple2<Object, Object> call(LabeledPoint p)
		    throws Exception {
		// TODO Auto-generated method stub
		Double score=model.predict(p.features());//���Լ�����
		return new Tuple2<Object, Object>(score, p.label());
	    }
	});//������Լ�����
	System.out.println(scoreAndLabels.collect());
	BinaryClassificationMetrics metrics=new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));//��ȡ����ָ��
	double auROC=metrics.areaUnderROC();
	System.out.println("Area under ROC="+auROC);
    }

}
