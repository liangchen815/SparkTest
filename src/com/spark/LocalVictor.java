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
	Vector dv=Vectors.dense(1.0,0.0,3.0);//�ܼ�����
	System.out.println(dv);
	Vector sv=Vectors.sparse(3, new int[]{0,2},new double[]{1.0,3.0});//ϡ�����
	System.out.println(sv);
	LabeledPoint pos=new LabeledPoint(1.0, dv);//�����ܼ�����
	System.out.println(pos);
	LabeledPoint neg=new LabeledPoint(0.0, sv);//����ϡ������
	System.out.println(neg);
	
    }

}
