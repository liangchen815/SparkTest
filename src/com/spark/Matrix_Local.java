package com.spark;

import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Matrices;

public class Matrix_Local {

    /**
     * @param args
     */
    public static void main(String[] args) {
	// TODO Auto-generated method stub
	Matrix dm=Matrices.dense(3, 2, new double[]{1.0,3.0,5.0,2.0,4.0,6.0});
	System.out.println(dm);
    }

}
