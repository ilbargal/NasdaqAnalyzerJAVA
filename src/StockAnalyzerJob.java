import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Writables.ClusterCenter;

import Canopy.CanopyMapper;
import Canopy.CanopyReducer;



public class StockAnalyzerJob {
	
	private static final Log LOG = LogFactory.getLog(StockAnalyzerJob.class);
	
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
		canopyJob.setMapOutputValueClass(ClusterCenter.class);
		    
		//Specify the job's output key and value classes. 
		canopyJob.setOutputKeyClass(ClusterCenter.class);
		canopyJob.setOutputValueClass(IntWritable.class);
		 
		@SuppressWarnings("unused")
		boolean success = canopyJob.waitForCompletion(true);
		//*****************Finish with Canopy, Starting calculate Centroids for each Canopy Center**********//
		
		//Creating random object in order to calculate Centroids of each ClusterCenter by his members ratio
		Random rand = new Random();
		int totalClustersNum= 7;
			
		// Canopy center and his close stock vectors
		HashMap<ClusterCenter,Integer> canopyDivisionOfCentroidsMap = new HashMap<ClusterCenter,Integer>();
		
		//Reading Canopy output in order to set them Centroids by data division.
		Path ClusterCentersPath = new Path(projectFolder + "/files/ClusterCenters/ClusterCenters.seq");
		long totalVectors = canopyJob.getCounters().findCounter(CanopyMapper.Counter.NUMBER_OF_VECTORS).getValue();
		//Reading the canopy Path with how much neighbors/All he have
		SequenceFile.Reader ClusterCentersReader = new SequenceFile.Reader(fs, ClusterCentersPath, jobConfigurations);
		ClusterCenter centerReader;
		IntWritable closeVectors;
		int sum = 0; //Summing all Centroids number for each Canopy Center in order to avoid mistakes.
		
		while (ClusterCentersReader.next(centerReader = new ClusterCenter() , closeVectors = new IntWritable())) {			
			Double close_vectors_propotion = new Double(closeVectors.get())/totalVectors;
			Double kmeans_cluster_proportion = close_vectors_propotion * totalClustersNum;
			//Removing Centers with low amount of data
			if(close_vectors_propotion>=1)
			{
				System.out.println(centerReader);
					canopyDivisionOfCentroidsMap.put(centerReader, (close_vectors_propotion).intValue());
					sum = sum + (close_vectors_propotion).intValue();
			}
		}
		ClusterCentersReader.close();
		
		// Create random files of kmeans centroids - close to the current canopy centers.
		// public ArrayList<ClusterCenter> createCloseClusterCenters(Hashmap canopyDivisionOfCentroidsMap)
		// Return arraylist with size = totalClustersNum (all K) of clustercenters which close to original canopy centers
		
		// Save them to file Path centerFile = new Path("files/kmeans-clustering/import/center/cen.seq");
		
		
		// Run Kmeans job
		KMeansClusteringJob.run();
		
		// Read final data from binary file and save it to normal file
		
	    System.exit(success ? 0 : 1);
	    
	    
	    // FORMAT TO ERAN :
	    // CLUSTER_ID, STOCK_NAME
	    // CLUSER_ID, STOCK_NAME
	}

}
