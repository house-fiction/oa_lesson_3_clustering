package com.oracle.hadoop.canopy;


import org.apache.commons.math.linear.*;
import java.lang.*;
import java.util.*;

public class Similarity
{

	public static double getDistance(String distanceType, String [] v1, String [] v2)
	{
	    if (distanceType.equals("jacard"))
		{
		    return jacardSimilarity(Arrays.asList(v1), Arrays.asList(v2));
		}
	    else if (distanceType.equals("cos"))
		{
		    return cosineDistance(toDoubleArray(v1), toDoubleArray(v2));
		}
	    else if (distanceType.equals("L1"))
		{
		    return oneNorm(toDoubleArray(v1), toDoubleArray(v2));
		}
	    else if (distanceType.equals("L2"))
		{
		    return twoNorm(toDoubleArray(v1), toDoubleArray(v2));
		}
	    else if (distanceType.equals("Inf"))
		{
		    return infNorm(toDoubleArray(v1), toDoubleArray(v2));
		}
	    else
		return 0.0;
	}


    public static double [] toDoubleArray(String [] s)
    {
	double [] d = new double[s.length];
	for (int i  = 0; i < s.length; i++)
	    {
		try
		    {
			double d_i = Double.parseDouble(s[i]);
			d[i] = d_i;
		    
		}
		catch (NumberFormatException NFE)
		    {
			d[i] = Double.NaN;
		    }
	    }
	return d;
    }

    public static double jacardSimilarity(List<String> s1, List<String> s2)
    {
	HashSet<String> unionXY = new HashSet<String>(s1);
	unionXY.addAll(s2);
	HashSet<String> intersectionXY = new HashSet<String>(s1);
	intersectionXY.retainAll(s2);

	double similarity = intersectionXY.size()/((double)unionXY.size());
	return similarity;
    }

    public static double cosineDistance(double [] x, double [] y)
    {
	ArrayRealVector x_v = new ArrayRealVector(x);
	ArrayRealVector y_v = new ArrayRealVector(y);
	double denominator  = Math.sqrt(x_v.getNorm()*y_v.getNorm());
	return x_v.dotProduct(y_v)/denominator;
    }

    public static double twoNorm(double [] x, double [] y)
    {
	ArrayRealVector x_v = new ArrayRealVector(x);
	ArrayRealVector y_v = new ArrayRealVector(y);
	return x_v.getDistance(y_v);

    }

    public static double oneNorm(double [] x, double [] y)
    {
	ArrayRealVector x_v = new ArrayRealVector(x);
	ArrayRealVector y_v = new ArrayRealVector(y);
	return x_v.getL1Distance(y_v);

    }

    public static double infNorm(double [] x, double [] y)
    {
	ArrayRealVector x_v = new ArrayRealVector(x);
	ArrayRealVector y_v = new ArrayRealVector(y);
	return x_v.getLInfDistance(y_v);
	
    }



}