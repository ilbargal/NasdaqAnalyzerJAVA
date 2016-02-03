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
import Writables.ClusterCenter;
import Writables.StockWritable;

public class CanopyReducer extends Reducer<IntWritable, ClusterCenter, ClusterCenter, IntWritable> {
	private List<ClusterCenter> canopyFinalCenters;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		this.canopyFinalCenters = new ArrayList<ClusterCenter>();
	}
	
	@Override
	protected void reduce(IntWritable key, Iterable<ClusterCenter> values, Context context)
			throws IOException, InterruptedException {
			
		for(final ClusterCenter valueCenter : values) {
			
			double distance = Integer.MAX_VALUE;
			
			boolean inCluster = false;
			for (ClusterCenter center : this.canopyFinalCenters) {
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
				ClusterCenter center = new ClusterCenter(valueCenter);
			    this.canopyFinalCenters.add(center);
			}	
		}
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
		
		//Write sequence file with results of Canopy Center, Number of connected vectors.
		FileSystem fs = FileSystem.get(context.getConfiguration());
		String rootFolder = context.getConfiguration().get("projectFolder");
		Path ClusterCentersPath = new Path(rootFolder + "/files/ClusterCenters/ClusterCenters.seq");
		context.getConfiguration().set("canopy.path",ClusterCentersPath.toString());
		
		@SuppressWarnings("deprecation")
		final SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs,context.getConfiguration(), ClusterCentersPath , ClusterCenter.class,IntWritable.class);
		for (ClusterCenter center : this.canopyFinalCenters) {
			centerWriter.append(center,new IntWritable(center.getCloseVectors()));
			context.write(center, new IntWritable(center.getCloseVectors()));
		}
		centerWriter.close();
	}
}
