package com.test;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

public class Test2 {

    /**
     * @param args
     */
    public static void main(String[] args) {
	// TODO Auto-generated method stub
	SparkConf conf=new SparkConf().setMaster("spark://localhost:7077").setAppName("test");
	JavaSparkContext sc=new JavaSparkContext(conf);
	JavaSQLContext sqlContext=new JavaSQLContext(sc);
	JavaSchemaRDD teenager1=sqlContext.sql("select * from people where age>19");
	List<String> list=teenager1.map(new Function<Row, String>() {

	    @Override
	    public String call(Row arg0) throws Exception {
		// TODO Auto-generated method stub
		return "name:"+arg0.getString(1);
	    }
	}).collect();
	System.out.println(list);
    }

}
