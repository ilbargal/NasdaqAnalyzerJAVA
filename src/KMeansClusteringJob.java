import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import Writables.ClusterCenter;
import Writables.StockWritable;
import Writables.Vector;

import KMeans.KMeansMapper;
import KMeans.KMeansReducer;


public class KMeansClusteringJob {
	
	private static final Log LOG = LogFactory.getLog(KMeansClusteringJob.class);
	private static final String JOB_NAME = "KMeans Clustering";
	
	public static void run() throws IOException,
			InterruptedException, ClassNotFoundException {

		int jobIteration = 1;
		Configuration conf = new Configuration();
		conf.set("num.iteration", jobIteration + " ");
		
		// Vectors file
		Path inputFile = new Path("files/kmeans-clustering/import/data");

		//  Centers file
		Path centerFile = new Path("files/kmeans-clustering/import/center/cen.seq");
		conf.set("centroid.path", centerFile.toString());
		
		// Output file
		Path outputFile = new Path("files/kmeans-clustering/depth_1");

		Job job = new Job(conf);
		job.setJobName(JOB_NAME);

		// Set the classes for the job
		job.setMapperClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);
		job.setJarByClass(KMeansMapper.class);
		
		FileSystem fs = FileSystem.get(conf);
		
		// Generate the input files (TODO: remove this line)
		//generateInputFiles(conf, fs, outputFile, centerFile, inputFile);

		// Set input and output files for the job
		SequenceFileOutputFormat.setOutputPath(job, outputFile);
		SequenceFileInputFormat.addInputPath(job, inputFile);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(ClusterCenter.class);
		job.setOutputValueClass(StockWritable.class);

		// Run job for the first time
		job.waitForCompletion(true);

		// Get the number of centers that were converged during the reducer
		long convCounter = job.getCounters()
				.findCounter(KMeansReducer.Counter.CONVERGED).getValue();
		jobIteration++;
		
		// Run job until no changes happened
		while (convCounter > 0) {
			LOG.info("***Iteration: " + jobIteration + " Number of converged centers: " + convCounter);
			
			// Create a new configuration for the new job
			conf = new Configuration();
			conf.set("centroid.path", centerFile.toString());
			conf.set("num.iteration", jobIteration + "");
			
			// Create a new job
			job = new Job(conf);
			job.setJobName(JOB_NAME + " " + jobIteration + "#");
			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
			job.setJarByClass(KMeansMapper.class);

			inputFile = new Path("files/kmeans-clustering/depth_" + (jobIteration - 1) + "/");
			outputFile = new Path("files/kmeans-clustering/depth_" + jobIteration);

			SequenceFileInputFormat.addInputPath(job, inputFile);
			if (fs.exists(outputFile))
				fs.delete(outputFile, true);

			SequenceFileOutputFormat.setOutputPath(job, outputFile);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setOutputKeyClass(ClusterCenter.class);
			job.setOutputValueClass(Vector.class);

			job.waitForCompletion(true);
			jobIteration++;
			
			// Get the number of centers that were converged during the reducer
			convCounter = job.getCounters()
					.findCounter(KMeansReducer.Counter.CONVERGED).getValue();
		}

		Path result = new Path("files/kmeans-clustering/depth_" + (jobIteration - 1) + "/");
		FileStatus[] stati = fs.listStatus(result);
		
		// Iterate over all the files in the output folder of the 
		// last job that ran
		for (FileStatus status : stati) {
			// If current file isn't a directory
			if (!status.isDir()) {
				Path path = status.getPath();
				//Path path = new Path("files/clustering/depth_3/part-r-00000");
				LOG.info("FOUND " + path.toString());
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, path,conf);
				ClusterCenter key = new ClusterCenter();
				Vector v = new Vector();
				
				
				while (reader.next(key, v)) {
					LOG.info(key + " / " + v);
				}
				
				reader.close();
			}
		}
		
		/*
		try{
			Path path = new Path("hdfs://localhost:54310/user/cloudera/files/clustering/depth_3/part-r-00000");
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
			ClusterCenter key = new ClusterCenter();
			Vector v = new Vector();
			while (reader.next(key, v)) {
				System.out.println(key + " / " + v);
			}
			reader.close();
		}catch(Exception e){e.printStackTrace();}
		*/
	}
	
	/**
	 * This function generates the input files, so we will have some data for debugging
	 * @param conf
	 * @param fs
	 * @param out
	 * @param center
	 * @param in
	 * @throws IOException
	 */
	private static void generateInputFiles(Configuration conf, FileSystem fs, Path out, Path center, Path in) throws IOException{
//		if (fs.exists(out))
//			fs.delete(out, true);
//
//		if (fs.exists(center))
//			fs.delete(out, true);
//
//		if (fs.exists(in))
//			fs.delete(out, true);
//
//		final SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs,
//				conf, center, ClusterCenter.class, IntWritable.class);
//		final IntWritable value = new IntWritable(0);
//		centerWriter.append(new ClusterCenter(new Vector(1, 1)), value);
//		centerWriter.append(new ClusterCenter(new Vector(5, 5)), value);
//		centerWriter.close();
//
//		final SequenceFile.Writer dataWriter = SequenceFile.createWriter(fs,
//				conf, in, ClusterCenter.class, Vector.class);
//		dataWriter
//				.append(new ClusterCenter(new Vector(0, 0)), new Vector(1, 2));
//		dataWriter.append(new ClusterCenter(new Vector(0, 0)),
//				new Vector(16, 3));
//		dataWriter
//				.append(new ClusterCenter(new Vector(0, 0)), new Vector(3, 3));
//		dataWriter
//				.append(new ClusterCenter(new Vector(0, 0)), new Vector(2, 2));
//		dataWriter
//				.append(new ClusterCenter(new Vector(0, 0)), new Vector(2, 3));
//		dataWriter.append(new ClusterCenter(new Vector(0, 0)),
//				new Vector(25, 1));
//		dataWriter
//				.append(new ClusterCenter(new Vector(0, 0)), new Vector(7, 6));
//		dataWriter
//				.append(new ClusterCenter(new Vector(0, 0)), new Vector(6, 5));
//		dataWriter.append(new ClusterCenter(new Vector(0, 0)), new Vector(-1,
//				-23));
//		dataWriter.close();
	}

}
