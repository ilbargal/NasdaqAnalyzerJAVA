package Canopy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import Writables.StockWritable;

public class CanopyCenter implements WritableComparable<CanopyCenter> {

	private StockWritable center;
	private IntWritable closeVectors;

	public CanopyCenter() {
		super();
		this.center = new StockWritable();
		this.closeVectors = new IntWritable(0);
	}

	public CanopyCenter(CanopyCenter canopyCenter) {
		this.center = new StockWritable(canopyCenter.center);
		this.closeVectors = new IntWritable(0);
		addConnectedStocks(canopyCenter.closeVectors.get());
	}

	public CanopyCenter(StockWritable stock) {
		this.center = stock;
		this.closeVectors = new IntWritable(0);
	}

	public StockWritable getCenter() {
		return this.center;
	}

	public int getCloseVectors() {
		return this.closeVectors.get();
	}

	public void addConnectedStock() {
		this.closeVectors = new IntWritable(this.closeVectors.get() + 1);
	}

	public void addConnectedStocks(int connectedStocks) {
		this.closeVectors = new IntWritable(this.closeVectors.get()
				+ connectedStocks);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		center.readFields(in);
		this.closeVectors.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		center.write(out);
		this.closeVectors.write(out);
	}
	
	public boolean converged(CanopyCenter c) {
		return compareTo(c) == 0 ? false : true;
	}

	@Override
	public int compareTo(CanopyCenter o) {
		return this.center.compareTo(o.center);
	}

	public boolean equals(Object o) {
		if (o instanceof CanopyCenter) {
			CanopyCenter other = (CanopyCenter) o;
			return this.center.equals(other.getCenter())
					&& this.closeVectors.equals(other.getCloseVectors());
		}
		return false;
	}

	@Override
	public int hashCode() {
		return this.closeVectors.hashCode() * this.center.hashCode() + hashCode();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "CanopyCenter:[\ncenter=" + this.center + "\nclose vectors=" + this.getCloseVectors() + "]";
	}
}
