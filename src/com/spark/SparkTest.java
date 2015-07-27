package com.spark;

/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

public class SparkTest {
  public static void main(String[] args) {
    String logFile = "hdfs://10.184.33.241:9000/wyh-dfs/output2/part-r-00000"; // Should be some file on your system
    SparkConf conf = new SparkConf().setMaster("local").setAppName("test");//Configuration for a Spark application. 
    JavaSparkContext sc = new JavaSparkContext(conf);//A Java-friendly version of SparkContext that returns JavaRDDs and works with Java collections instead of Scala ones.
    JavaRDD<String> logData = sc.textFile(logFile).cache();
    long numAs = logData.filter(new Function<String, Boolean>() {//Return a new RDD containing only the elements that satisfy a predicate.
      public Boolean call(String s) { return s.contains("a"); }
    }).count();
    long numBs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("b"); }
    }).count();
    System.out.println(logData.collect().toString());
    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
  }
}
