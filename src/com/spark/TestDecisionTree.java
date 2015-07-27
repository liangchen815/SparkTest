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
		.toJavaRDD().cache();// cache重新计算时无需加载
	// JavaRDD<LabeledPoint> training=data.sample(false,
	// 0.7);//随机数种子默认当前时间，所以每次运行产生的随机数都不同
	// JavaRDD<LabeledPoint> test=data.subtract(training);
	int numClasses = 6;// label{0,1...numClasses-1},所以numClasses=6，label=0为空集
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
	// });//预测值和label
	// JavaPairRDD<Double, Double> filterData=predictionAndLabel.filter(new
	// Function<Tuple2<Double,Double>, Boolean>() {
	//
	// @Override
	// public Boolean call(Tuple2<Double, Double> p1) throws Exception {

	// return !p1._1().equals(p1._2);
	// }
	// });//过滤出预测值和label不同的点
	// System.out.println("Error Data(预测值，实际值):"+filterData.collect());
	// Double testRight=1-1.0*filterData.count()/test.count();
	// System.out.println("Test Right:"+testRight);
	 System.out.println("Learned classfication tree model:\n"+model);

	// 开始画图
	int Xmax = 1;// x轴实际长度
	int Ymax = 12;// y轴实际长度
	int max = 100;//
	List<Vector> list = new ArrayList<Vector>();
	for (int i = 0; i <= max; i++) {
	    for (int j = 0; j <= max; j++) {
		Double x = 1.0 * Xmax * i / max;// 实际x坐标值
		Double y = 1.0 * Ymax * j / max;// 实际y坐标值
		Vector point = Vectors.dense(x, y);
		list.add(point);
	    }
	}
//	System.out.println("坐标点：" + list);

	JavaRDD<Vector> example = sc.parallelize(list).cache();
//	System.out.println(example.collect());
	JavaRDD<LabeledPoint> predict = example
		.map(new Function<Vector, LabeledPoint>() {

		    @Override
		    public LabeledPoint call(Vector point) throws Exception {
			return new LabeledPoint(model.predict(point), point);
		    }
		}).cache();
//	System.out.println("预测值：" + predict.collect());

	List<LabeledPoint> listpredict = predict.collect();// 将RDD转为List,所有max*max个点

	Map<Double[], Double> map = new HashMap<>();// (坐标，值)
	for (LabeledPoint labeledPoint : listpredict) {
	    Double x = labeledPoint.features().apply(0);
	    Double y = labeledPoint.features().apply(1);
	    Double[] point = new Double[] { x, y };
	    Double label = labeledPoint.label();
	    map.put(point, label);
	}

	Set<Double[]> pointset = map.keySet();// 取出坐标集合
	Double stepX = 1.0 * Xmax / max;// X步长
	Double stepY = 1.0 * Ymax / max;// Y步长
	Map<Double[], Double> linemap = new HashMap<>();// 描线的点集合

	// 排除上下左右一样的点，留下边线
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
	System.out.println("-------------边线------------------");
	while (iterator.hasNext()) {
	    Double[] key = iterator.next();
	    Double val = linemap.get(key);
	    System.out.println("坐标：" + Arrays.asList(key) + " 预测值：" + val);
	}
    }

}
