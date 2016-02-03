package Canopy;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.hamcrest.core.Is;
import org.junit.internal.matchers.IsCollectionContaining;

import Writables.*;
import Writables.Vector.VectorType;

public class CanopyMapper extends Mapper<LongWritable, Text, IntWritable, CanopyCenter> {
	private final int OPEN_STOCK_TYPE_INDEX = 0;
	private final int HIGH_STOCK_TYPE_INDEX = 1;
	private final int LOW_STOCK_TYPE_INDEX = 2;
	private final int CLOSE_STOCK_TYPE_INDEX = 3;
	
	private List<CanopyCenter> canopyCenters;
	
	public static enum Counter{
		NUMBER_OF_VECTORS;
	}
	
	 @Override
	 public void setup(Context context) throws IOException,InterruptedException {
		 super.setup(context);
		 this.canopyCenters = new ArrayList<CanopyCenter>();
	 }
	 
	 @Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StockWritable stock = initalizeStock(value.toString());
		
		double distance;
		context.getCounter(Counter.NUMBER_OF_VECTORS).increment(1);
		boolean inCluster = false;
		
		for (CanopyCenter center : this.canopyCenters) {
			distance = DistanceMeasurer.measureDistance(center.getCenter(), stock);
			
			if ( distance<= DistanceMeasurer.T1 ) {
				inCluster = true;
				
				// Check T2
				if(distance >= DistanceMeasurer.T2) {
					center.addConnectedStock();
				}
				break;
			}
		}
		
		// If there are'nt canopy centers or the stock vector is'nt close to any canopy centers
		// Set current stock vector as canopy center itself
		if (!inCluster) {
		   	CanopyCenter center = new CanopyCenter(stock);
		    this.canopyCenters.add(center);
		    center.addConnectedStock();
		}
	}
	 
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context); 
 		for (int center = 0; center < this.canopyCenters.size(); center++) {
 			context.write(new IntWritable(1), this.canopyCenters.get(center));
		}
	} 
	 
	private StockWritable initalizeStock(String line) {
		StringTokenizer tokenizer = new StringTokenizer(line);
		int days = (tokenizer.countTokens() - 1) / 4;
		int dayIndex = 0;
		
		String stockName = "";
		double[][] stockData = new double[4][days];
		
		// Set name
		if (tokenizer.hasMoreTokens()) {
			
			stockName = tokenizer.nextToken();
			
			// Set data
			while (tokenizer.hasMoreTokens()){
				if (dayIndex > days) break;
				
				stockData[OPEN_STOCK_TYPE_INDEX][dayIndex] = Double.parseDouble(tokenizer.nextToken());
				stockData[HIGH_STOCK_TYPE_INDEX][dayIndex] = Double.parseDouble(tokenizer.nextToken());
				stockData[LOW_STOCK_TYPE_INDEX][dayIndex] = Double.parseDouble(tokenizer.nextToken());
				stockData[CLOSE_STOCK_TYPE_INDEX][dayIndex] = Double.parseDouble(tokenizer.nextToken());
				dayIndex++;
			}
		}
		
		StockWritable result = new StockWritable(new Text(stockName));
		result.getStockVector(VectorType.OPEN).set(stockData[OPEN_STOCK_TYPE_INDEX]);
		result.getStockVector(VectorType.HIGH).set(stockData[HIGH_STOCK_TYPE_INDEX]);
		result.getStockVector(VectorType.LOW).set(stockData[LOW_STOCK_TYPE_INDEX]);
		result.getStockVector(VectorType.CLOSE).set(stockData[CLOSE_STOCK_TYPE_INDEX]);
		
		return result;
	}
}