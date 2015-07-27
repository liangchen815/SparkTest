package com.test;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

public class Test {

    /**
     * @param args
     */
    public static void main(String[] args) {
	SparkConf conf=new SparkConf().setMaster("spark://localhost:7077").setAppName("test");
	JavaSparkContext sc=new JavaSparkContext(conf);
	JavaSQLContext sqlContext=new JavaSQLContext(sc);
	JavaRDD<Person> people=sc.textFile("E:/Hadoop/spark-1.4.0-bin-hadoop2.6/spark-1.4.0-bin-hadoop2.6/examples/src/main/resources/people.txt").map(new Function<String, Person>() {

	    @Override
	    public Person call(String arg0) throws Exception {
		// TODO Auto-generated method stub
		String[] parts=arg0.split(",");
		Person person=new Person();
		person.setName(parts[0]);
		person.setAge(Integer.parseInt(parts[1].trim()));
		return person;
	    }
	});
	JavaSchemaRDD schemaPeople=sqlContext.applySchema(people, Person.class);
	schemaPeople.registerTempTable("people");
	System.out.println(schemaPeople.schemaString());
	JavaSchemaRDD teenager=sqlContext.sql("select * from people where age<=19").cache();
	List<String> listPersons=teenager.map(new Function<Row, String>() {

	    @Override
	    public String call(Row arg0) throws Exception {
		// TODO Auto-generated method stub
		return "name:"+arg0.getString(1);
	    }
	}).collect();
	while (true) {
	    System.out.println(listPersons);
	}
	
    }

}
