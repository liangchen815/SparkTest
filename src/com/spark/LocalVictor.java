package com.spark;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

public class LocalVictor {

    /**
     * @param args
     */
    /**
     * @param args
     */
    public static void main(String[] args) {
	// TODO Auto-generated method stub
	Vector dv=Vectors.dense(1.0,0.0,3.0);//密集向量
	System.out.println(dv);
	Vector sv=Vectors.sparse(3, new int[]{0,2},new double[]{1.0,3.0});//稀疏矩阵
	System.out.println(sv);
	LabeledPoint pos=new LabeledPoint(1.0, dv);//正向密集向量
	System.out.println(pos);
	LabeledPoint neg=new LabeledPoint(0.0, sv);//负向稀疏向量
	System.out.println(neg);
	
    }

}
