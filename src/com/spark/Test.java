package com.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
import scala.collection.generic.BitOperations.Int;

public class Test {
    private static final Pattern SPACES = Pattern.compile("\\s+");
    /**
     * @param args
     */
    public static void main(String[] args) {
	// TODO Auto-generated method stub
	SparkConf conf=new SparkConf().setAppName("Test").setMaster("local");
	JavaSparkContext jsc=new JavaSparkContext(conf);
	List<Integer> data = Arrays.asList(1,2,3,4,5);
	JavaRDD<Integer> distData= jsc.parallelize(data);
	JavaRDD<Integer> test=distData.map(new Function<Integer, Integer>() {

	    @Override
	    public Integer call(Integer arg0) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("map:"+arg0);
		return arg0;
	    }
	});
	
	int d=test.reduce(new Function2<Integer, Integer, Integer>() {
	    
	    @Override
	    public Integer call(Integer arg0, Integer arg1) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("reduce-arg0:"+arg0);
		System.out.println("reduce-arg1:"+arg1);
		System.out.println(arg0+arg1+":reduceReturn");
		return arg0+arg1;
	    }
	});
//	System.out.println("reduceResult："+d);
//	System.out.println("count:"+test.count());
//	System.out.println("collect:"+test.collect());
//	System.out.println("first:"+test.first());
//	System.out.println("take:"+test.take(2));
//	System.out.println("takeSample:"+test.takeSample(false, 2));
//	System.out.println("takeOrdered:"+test.takeOrdered(4));
//	test.saveAsTextFile("C:/saveA.txt");
//	test.saveAsObjectFile("C:/saveB.txt");
//	System.out.println("context"+test.context());
	
	test.foreach(new VoidFunction<Integer>() {
	    int a=0;
	    @Override
	    public void call(Integer arg0) throws Exception {
		// TODO Auto-generated method stub
		a+=1;
		System.out.println("累加器："+a);
	    }
	});
	
	//过滤
	JavaRDD<Integer> filterTest=test.filter(new Function<Integer, Boolean>() {

	    @Override
	    public Boolean call(Integer arg0) throws Exception {
		// TODO Auto-generated method stub]
		return arg0>3;
	    }
	});
	System.out.println("filter:"+filterTest.collect());
	
	//flatMapTest  一个输入元素被映射为0或者多个输出元素
	JavaRDD<Integer> flatMapTest=test.flatMap(new FlatMapFunction<Integer, Integer>() {

	    @Override
	    public Iterable<Integer> call(Integer arg0) throws Exception {
		// TODO Auto-generated method stub
		List<Integer> list=new ArrayList<Integer>();
		for (int i = 0; i < 5; i++) {
		    list.add(arg0);
		}
		return (Iterable<Integer>) list;
	    }
	});
	System.out.println("flatMapTest:"+flatMapTest.collect());
	
//	JavaRDD<Integer> mapPartitionsTest=test.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
//
//	    @Override
//	    public Iterable<Integer> call(Iterator<Integer> arg0) throws Exception {
//		// TODO Auto-generated method stub
//		System.out.println("-----------"+arg0);
//		return null;
//	    }
//	});
//	System.out.println("mapPartitionsTest"+mapPartitionsTest.collect());
	
	//随机采样sample
	JavaRDD<Integer> sampleTest=test.sample(false, 0.4);
	System.out.println("sampleTest:"+sampleTest.collect());
	
	//两个数据集联合Union
	JavaRDD<Integer> unionTest=test.union(filterTest);
	System.out.println("unionTest:"+unionTest.collect());
	
	//过滤掉重复元素distinct
	JavaRDD<Integer> distinctTest=unionTest.distinct();
	System.out.println("distinctTest:"+distinctTest.collect());
	
	
    }

}
