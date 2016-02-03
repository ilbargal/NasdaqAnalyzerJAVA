package Canopy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.math.util.OpenIntToFieldHashMap.Iterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;

import Canopy.CanopyMapper.Counter;
import Writables.StockWritable;

public class CanopyReducer extends Reducer<IntWritable, CanopyCenter, CanopyCenter, IntWritable> {
	private List<CanopyCenter> canopyFinalCenters;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		this.canopyFinalCenters = new ArrayList<CanopyCenter>();
	}
	
	@Override
	protected void reduce(IntWritable key, Iterable<CanopyCenter> values, Context context)
			throws IOException, InterruptedException {
			
		for(final CanopyCenter valueCenter : values) {
			
			double distance = Integer.MAX_VALUE;
			
			boolean inCluster = false;
			for (CanopyCenter center : this.canopyFinalCenters) {
				distance = DistanceMeasurer.measureDistance(center.getCenter(), valueCenter.getCenter());
				
				if ( distance<= DistanceMeasurer.T1 ) {
					inCluster = true;    				 
					if(distance >= DistanceMeasurer.T2) {
						center.addConnectedStocks(valueCenter.getCloseVectors());
					}
					break;
				}
			}
			
			// If there are'nt canopy centers or the stock vector is'nt close to any canopy centers already
			// Set current stock vector as canopy center itself
			if (!inCluster) {
			   	CanopyCenter center = new CanopyCenter(valueCenter);
			    this.canopyFinalCenters.add(center);
			}	
		}
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
		
//		//Write sequence file with results of Canopy Center, Number of connected vectors.
//		FileSystem fs = FileSystem.get(context.getConfiguration());
//		String rootFolder = context.getConfiguration().get("projectFolder");
//		System.out.println(rootFolder);
//		Path canopy = new Path(rootFolder + "/files/CanopyCenters/canopyCenters.seq");
//		context.getConfiguration().set("canopy.path",canopy.toString());
//		
//		final SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs,context.getConfiguration(), canopy , CanopyCenter.class,IntWritable.class);
		for (CanopyCenter center : this.canopyFinalCenters) {
//			centerWriter.append(center,new IntWritable(center.getCloseVectors()));
			context.write(center, new IntWritable(center.getCloseVectors()));
		}
//		centerWriter.close();	
	}
}
