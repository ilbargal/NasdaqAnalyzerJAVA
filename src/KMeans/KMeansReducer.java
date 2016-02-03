package KMeans;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;

import Writables.ClusterCenter;
import Writables.StockWritable;



// calculate a new clustercenter for these vertices
public class KMeansReducer extends
		Reducer<ClusterCenter, StockWritable, ClusterCenter, StockWritable> {

	public static enum Counter {
		CONVERGED
	}

	List<ClusterCenter> centers = new LinkedList<ClusterCenter>();

	/**
	 * Calculate a new center for each set of values
	 */
	@Override
	protected void reduce(ClusterCenter key, Iterable<StockWritable> values,
			Context context) throws IOException, InterruptedException {

		StockWritable newCenter = new StockWritable(key.getCenter());
		List<StockWritable> stocksVectorList = new LinkedList<StockWritable>();
		
		// Calculate the sum
		for (StockWritable value : values) {
			stocksVectorList.add(new StockWritable(value));
			
			for (int index = 0; index < 4; index++) {
				for (int i = 0; i < value.getStockVector(index).getVector().length; i++) {
					newCenter.getStockVector(index).getVector()[i] += value.getStockVector(index).getVector()[i];
				}
			}
		}
		
		// Calculate the average
		for (StockWritable value : values) {
			stocksVectorList.add(new StockWritable(value));
			
			for (int index = 0; index < 4; index++) {
				for (int i = 0; i < value.getStockVector(index).getVector().length; i++) {
					newCenter.getStockVector(index).getVector()[i] = value.getStockVector(index).getVector()[i] / stocksVectorList.size();
				}
			}
		}

		// Create the new center
		ClusterCenter center = new ClusterCenter(newCenter);
		centers.add(center);
		
		for (StockWritable vector : stocksVectorList) {
			context.write(center, vector);
		}

		// If the center has changed, increment the converged counter
		if (center.converged(key))
			context.getCounter(Counter.CONVERGED).increment(1);

	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		
		// Delete the centroid path
		Configuration conf = context.getConfiguration();
		Path outPath = new Path(conf.get("centroid.path"));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outPath, true);
		
		final SequenceFile.Writer out = SequenceFile.createWriter(fs,
				context.getConfiguration(), outPath, ClusterCenter.class,
				IntWritable.class);
		final IntWritable value = new IntWritable(0);
		
		// Output the centers
		for (ClusterCenter center : centers) {
			out.append(center, value);
		}
		
		out.close();
	}
}
