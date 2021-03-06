package com.oracle.hadoop.canopy;



import java.io.IOException;

import java.util.*;
import java.nio.CharBuffer;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



/* assume we're taking in CSV data for now */
public class FindCanopies
{

    public static class FindCanopiesMapper
	extends Mapper<LongWritable, Text, IntWritable, Text> {

	private int count = 0;
	private String sep;
	private ArrayList<String[]> canopies = new ArrayList<String[]>();

	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
	    Configuration conf = context.getConfiguration();
	    double T2 = Double.parseDouble(conf.get("T2"));
	    sep = conf.get("sep");
	    String distanceType = conf.get("distance");
	    
	    String [] thisVector = value.toString().split(sep);
	    
	    for (String [] canopy : canopies)
		{
		    double distance = Similarity.getDistance(distanceType, thisVector, canopy);
		    //System.out.println("distance: "+distance);
		    if (distance < T2)
			return;
		    
		    count += 1;

		}
	    canopies.add(thisVector);
	    
	}

	protected void cleanup(Context context) throws InterruptedException
	{
	    for (int index = 0; index < canopies.size(); index++)	       
	    {
		    String [] canopy = canopies.get(index);

		    StringBuilder builder = new StringBuilder();
		    for (int i = 0 ; i < canopy.length; i++)
			{
			    builder.append(canopy[i]);
			    if (i < (canopy.length-1))
				builder.append("\t");
			}
		    try
			{			  
		
			    context.write(new IntWritable(index), new Text(builder.toString()));
			}
		    catch (IOException failToMap)
			{ failToMap.printStackTrace();
			}


		}
	    

	}
    }

    public static class FindCanopiesReducer
	extends Reducer<IntWritable, Text, IntWritable, Text> {
	private int count = 0;
	private ArrayList<String[]> dedupedCanopies = new ArrayList<String[]>();
	private String sep;

	public void reduce(IntWritable key, Text value, Context context) throws IOException, InterruptedException
	{
	    Configuration conf = context.getConfiguration();
	    double T2 = Double.parseDouble(conf.get("T2"));
	    sep = conf.get("sep");
	    String distanceType = conf.get("distance");
	    
	    String [] thisVector = value.toString().split(sep);
	    
	    for (String [] canopy : dedupedCanopies)
		{
		    double distance = Similarity.getDistance(distanceType, thisVector, canopy);
		    if (distance < T2)
			return;
		    

		}
	    dedupedCanopies.add(thisVector);

	}

	protected void cleanup(Context context) throws InterruptedException
	{
	    //System.out.println("cleanup: reducer");
	    for (int index = 0; index < dedupedCanopies.size(); index++)       
		{
		    String [] canopy = dedupedCanopies.get(index);

		    StringBuilder builder = new StringBuilder();
		    for (int i = 0 ; i < canopy.length; i++)
			{
			    builder.append(canopy[i]);
			    if (i < (canopy.length-1))
				builder.append(sep);
			}
		    try
			{			   

			    context.write(new IntWritable(index), new Text(builder.toString()));
			}
		    catch (IOException failToMap)
			{ failToMap.printStackTrace();
			}


			}
	    
	}
       
    }

    public static void main(String[] args) throws Exception {
	
      Configuration conf = new Configuration();
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      Job job = new Job(conf, "canopy cluster");
      job.setJarByClass(FindCanopies.class);
      job.setMapperClass(FindCanopiesMapper.class);
      job.setReducerClass(FindCanopiesReducer.class);
      
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
      FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0:1);;


  }
}
    
