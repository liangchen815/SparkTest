package com.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;

public class TestDecisionTree {

    /**
     * @param args
     */
    public static void main(String[] args) {

	SparkConf conf = new SparkConf().setMaster("local").setAppName(
		"DecisionTree");
	JavaSparkContext sc = new JavaSparkContext(conf);
	String datapath = "C:/Users/zhengliang.wu/Desktop/2D_MC_TEST2.txt";
	JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), datapath)
		.toJavaRDD().cache();// cache���¼���ʱ�������
	// JavaRDD<LabeledPoint> training=data.sample(false,
	// 0.7);//���������Ĭ�ϵ�ǰʱ�䣬����ÿ�����в��������������ͬ
	// JavaRDD<LabeledPoint> test=data.subtract(training);
	int numClasses = 6;// label{0,1...numClasses-1},����numClasses=6��label=0Ϊ�ռ�
	HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
	String impurity = "gini";
	int maxDepth = 3;
	int maxBins = 7;
	final DecisionTreeModel model = DecisionTree.trainClassifier(data,
		numClasses, categoricalFeaturesInfo, impurity, maxDepth,
		maxBins);
	// JavaPairRDD<Double, Double> predictionAndLabel=test.mapToPair(new
	// PairFunction<LabeledPoint, Double, Double>() {
	//
	// @Override
	// public Tuple2<Double, Double> call(LabeledPoint p)
	// throws Exception {

	// return new Tuple2<Double, Double>(model.predict(p.features()),
	// p.label());
	// }
	// });//Ԥ��ֵ��label
	// JavaPairRDD<Double, Double> filterData=predictionAndLabel.filter(new
	// Function<Tuple2<Double,Double>, Boolean>() {
	//
	// @Override
	// public Boolean call(Tuple2<Double, Double> p1) throws Exception {

	// return !p1._1().equals(p1._2);
	// }
	// });//���˳�Ԥ��ֵ��label��ͬ�ĵ�
	// System.out.println("Error Data(Ԥ��ֵ��ʵ��ֵ):"+filterData.collect());
	// Double testRight=1-1.0*filterData.count()/test.count();
	// System.out.println("Test Right:"+testRight);
	 System.out.println("Learned classfication tree model:\n"+model);

	// ��ʼ��ͼ
	int Xmax = 1;// x��ʵ�ʳ���
	int Ymax = 12;// y��ʵ�ʳ���
	int max = 100;//
	List<Vector> list = new ArrayList<Vector>();
	for (int i = 0; i <= max; i++) {
	    for (int j = 0; j <= max; j++) {
		Double x = 1.0 * Xmax * i / max;// ʵ��x����ֵ
		Double y = 1.0 * Ymax * j / max;// ʵ��y����ֵ
		Vector point = Vectors.dense(x, y);
		list.add(point);
	    }
	}
//	System.out.println("����㣺" + list);

	JavaRDD<Vector> example = sc.parallelize(list).cache();
//	System.out.println(example.collect());
	JavaRDD<LabeledPoint> predict = example
		.map(new Function<Vector, LabeledPoint>() {

		    @Override
		    public LabeledPoint call(Vector point) throws Exception {
			return new LabeledPoint(model.predict(point), point);
		    }
		}).cache();
//	System.out.println("Ԥ��ֵ��" + predict.collect());

	List<LabeledPoint> listpredict = predict.collect();// ��RDDתΪList,����max*max����

	Map<Double[], Double> map = new HashMap<>();// (���ֵ꣬)
	for (LabeledPoint labeledPoint : listpredict) {
	    Double x = labeledPoint.features().apply(0);
	    Double y = labeledPoint.features().apply(1);
	    Double[] point = new Double[] { x, y };
	    Double label = labeledPoint.label();
	    map.put(point, label);
	}

	Set<Double[]> pointset = map.keySet();// ȡ�����꼯��
	Double stepX = 1.0 * Xmax / max;// X����
	Double stepY = 1.0 * Ymax / max;// Y����
	Map<Double[], Double> linemap = new HashMap<>();// ���ߵĵ㼯��

	// �ų���������һ���ĵ㣬���±���
	for (Double[] double1 : pointset) {
	    Double x = double1[0];
	    Double y = double1[1];
	    Double p = map.get(double1);
	    Double[] left = new Double[] { x - stepX, y };
	    Double[] right = new Double[] { x + stepX, y };
	    Double[] down = new Double[] { x, y - stepY };
	    Double[] up = new Double[] { x, y + stepY };
	    Boolean todel = false;
	    if (pointset.contains(left) && map.get(left) == p) {
		if (pointset.contains(right) && map.get(right) == p) {
		    if (pointset.contains(down) && map.get(down) == p) {
			if (pointset.contains(up) && map.get(up) == p) {
			    todel = true;
			}
		    }
		}
	    }
	    if (!todel) {
		linemap.put(double1, p);
	    }
	}

	Set<Double[]> keySet = linemap.keySet();
	Iterator<Double[]> iterator = keySet.iterator();
	System.out.println("-------------����------------------");
	while (iterator.hasNext()) {
	    Double[] key = iterator.next();
	    Double val = linemap.get(key);
	    System.out.println("���꣺" + Arrays.asList(key) + " Ԥ��ֵ��" + val);
	}
    }

}
