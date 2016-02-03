import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Canopy.CanopyCenter;
import Canopy.CanopyMapper;
import Canopy.CanopyReducer;



public class StackAnalyzerJob {
	
	private static final Log LOG = LogFactory.getLog(StackAnalyzerJob.class);
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		String projectFolder = args[0];
		LOG.info("Project folder: "+ projectFolder);
		
		Path canopyInputPath = new Path(projectFolder + "/inputs");
		Path canopyOutputPath = new Path(projectFolder+"/canopyOutput");
		Configuration jobConfigurations = new Configuration();
		FileSystem fs = FileSystem.get(jobConfigurations);
		
		if (fs.exists(canopyOutputPath)){fs.delete(canopyOutputPath, true);}
		
		//Running Canopy Clustering Job
		Job canopyJob = new Job(jobConfigurations);
		canopyJob.getConfiguration().set("projectFolder",projectFolder);
		
		//Specify the jar file that contains Driver, Mapper and Reducer.
		canopyJob.setJarByClass(CanopyMapper.class);
		    
		//Configure Canopy Clustering Job name
		canopyJob.setJobName("Canopy Clustering");

		//Specify the paths to the input and output data based on the
		FileInputFormat.setInputPaths(canopyJob, canopyInputPath);
		FileOutputFormat.setOutputPath(canopyJob,canopyOutputPath);

		//Specify the Mapper and Reducer classes.
		canopyJob.setMapperClass(CanopyMapper.class);
		canopyJob.setReducerClass(CanopyReducer.class);
		canopyJob.setMapOutputKeyClass(IntWritable.class);
		canopyJob.setMapOutputValueClass(CanopyCenter.class);
		    
		//Specify the job's output key and value classes. 
		canopyJob.setOutputKeyClass(CanopyCenter.class);
		canopyJob.setOutputValueClass(IntWritable.class);
		 
		@SuppressWarnings("unused")
		boolean success = canopyJob.waitForCompletion(true);
		//*****************Finish with Canopy, Starting calculate Centroids for each Canopy Center**********//
		
		//Creating random object in order to calculate Centroids of each ClusterCenter by his members ratio
		Random rand = new Random();
		int totalClustersNum= 7;
			
//		//HashMap that will hold for each cluster center his number of Centroids to calculate
//		HashMap<CanopyCenter,Integer> canopyDivisionOfCentroidsMap = new HashMap<CanopyCenter,Integer>();
//		
//		//Reading Canopy output in order to set them Centroids by data division.
//		Path canopyPath = new Path(projectFolder + "/files/CanopyClusterCenters/canopyCenters.seq");
//		long totalVectors = canopyJob.getCounters().findCounter(CanopyMapper.Counter.NUMBER_OF_VECTORS).getValue();
//		//Reading the canopy Path with how much neighbors/All he have
//		SequenceFile.Reader canopyCentersReader = new SequenceFile.Reader(fs, canopyPath, jobConfigurations);
//		CanopyCenter centerReader ;
//		IntWritable neighbors ;
//		int sum = 0; //Summing all Centroids number for each Canopy Center in order to avoid mistakes.
		
	    System.exit(success ? 0 : 1);
	}

}
