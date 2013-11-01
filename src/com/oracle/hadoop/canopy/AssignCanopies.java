package com.oracle.hadoop.canopy;



import java.io.IOException;

import java.util.*;
import java.nio.CharBuffer;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



/* assume we're taking in CSV data for now */
public class AssignCanopies
{

    public static class AssignCanopiesMapper
	extends Mapper<LongWritable, Text, IntWritable, Text> {

	private int count = 0;
	private String sep;
	private HashMap<Integer, Canopy> canopies = new HashMap<Integer,Canopy>();
	private HashMap<Canopy, Integer> assignments = new HashMap<Canopy,Integer>();

	public void readCanopies(LocatedFileStatus f, Context context, Configuration conf)
	{
	    sep = conf.get("sep");
	    
	    // get the path back
	    Path thisFilePath = f.getPath();
	    // get a FileSplit for this file
	    FileSplit split = new FileSplit(thisFilePath, 0, f.getLen(), null);
	    try
		{
		    // for this file splice
		    KeyValueLineRecordReader reader = new KeyValueLineRecordReader(conf);
	    
		    reader.initialize(split, context);
		    while (reader.nextKeyValue())
			{
			    int cid = Integer.parseInt(reader.getCurrentKey().toString());
			    String vin = reader.getCurrentValue().toString();
			    String [] v = vin.split("\t");
			    System.out.println(v.length);
			    Canopy c = new Canopy(cid, v);
			    canopies.put(new Integer(cid),c);
			}
		}
	    catch (IOException ioe)
		{
		    ioe.printStackTrace();
		}
	    System.out.println(canopies.size() + " canopies loaded");
	}

	public void setup(Context context)
	{
	    // We need to read in the canopies in order to join them
	    // to the incoming record set
	    Configuration conf = context.getConfiguration();
	    try{
		String canopyDirectory = conf.get("canopies");
		Path canopyPath = new Path(canopyDirectory);
		FileSystem canopyFs = FileSystem.get(conf);
		if (canopyFs.exists(canopyPath))
		    {
			RemoteIterator<LocatedFileStatus> files = canopyFs.listFiles(canopyPath, false);
			
			while (files.hasNext())
			    {
				LocatedFileStatus f = files.next();
				readCanopies(f, context, conf);
			    }
		    
			
		}
	    }
	    catch (IOException ioe)
			{
			    ioe.printStackTrace();
			}

	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
	    Configuration conf = context.getConfiguration();
	    double T2 = Double.parseDouble(conf.get("T2"));
	    double T1 = Double.parseDouble(conf.get("T1"));
	    int idField = Integer.parseInt(conf.get("idField"));
	    sep = conf.get("sep");

	    String distanceType = conf.get("distance");
	    String [] inputA = value.toString().split(sep);
	    String [] thisVector = new String[inputA.length - 1];
	    String id = null;
	    int vindex =0;
	    for (int i = 0; i < inputA.length; i++)
		{
		    if (i == idField)
			{
			    id = inputA[i];
			}
		    else
			{
			    thisVector[vindex] = inputA[i];
			    vindex++;
			}
		}


	    for (Integer cID : canopies.keySet())
		{
		    Canopy canopy = canopies.get(cID);

		    double distance = Similarity.getDistance(distanceType, thisVector, canopy.canopyVector);
		    if (distance < T2)
			{
			    // this is too close to a center, toss it out
			    return;
			}
		    if (distance > T2 && distance < T1)
			{
			    // this is not tightly bound, but inside the canopy
			    // try to assign it to the canopy
			    Canopy thisPoint = new Canopy(Integer.parseInt(id), thisVector);
			    
			    if (!assignments.containsKey(thisPoint))
				{
				    assignments.put(thisPoint, new Integer(canopy.canopyId));
				}
			    else
				{
				    Integer existingAssignmentId = assignments.get(thisPoint);
				    Canopy existingCanopy = canopies.get(existingAssignmentId);
				    double oldDistance = Similarity.getDistance(distanceType, thisVector, existingCanopy.canopyVector);
				    
				    // if this is a closer assignment, take it
				    if (distance < oldDistance)
					{
					    assignments.put(thisPoint, cID);
					}
				}
			    
			}
		    
		}
	    

	    
	}

	protected void cleanup(Context context) throws InterruptedException
	{

	    // now that we're done, we want to emit all the assignments
	    for (Canopy c : assignments.keySet())
		{
		    // the vector id gets written out as an IntWritable
		    // the remainder is tab-separated
		    StringBuilder sb = new StringBuilder();
		    for (int i = 0; i < c.canopyVector.length; i++)
			{
			    sb.append(c.canopyVector[i]);
			    sb.append("\t");
			}
		    // add the canopy assigment
		    sb.append(assignments.get(c).toString());
		    try
			{
			    context.write(new IntWritable(c.canopyId), new Text(sb.toString()));
			}

		    catch (IOException writeException)
			{
			    writeException.printStackTrace();
			}
		    
		}
	    

	}
    }

    public static class AssignCanopiesReducer
	extends Reducer<IntWritable, Text, IntWritable, Text> {
	private int count = 0;
	private ArrayList<String[]> dedupedCanopies = new ArrayList<String[]>();
	private String sep;

	public void reduce(IntWritable key, Text value, Context context) throws IOException, InterruptedException
	{
	    Configuration conf = context.getConfiguration();
	    // Identity reducer just emit keys and values;
	    context.write(key, value);


	}

	protected void cleanup(Context context) throws InterruptedException
	{

	}
       
    }

    public static void main(String[] args) throws Exception {
	
      Configuration conf = new Configuration();
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      Job job = new Job(conf, "canopy cluster");
      job.setJarByClass(AssignCanopies.class);
      job.setMapperClass(AssignCanopiesMapper.class);
      job.setReducerClass(AssignCanopiesReducer.class);
      
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
      FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0:1);;


  }
}
    