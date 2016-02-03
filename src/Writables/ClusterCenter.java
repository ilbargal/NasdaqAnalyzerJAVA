package Writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;


public class ClusterCenter implements WritableComparable<ClusterCenter> {

	private StockWritable center;
	private IntWritable closeVectors;

	public ClusterCenter() {
		super();
		this.center = new StockWritable();
		this.closeVectors = new IntWritable(0);
	}

	public ClusterCenter(ClusterCenter canopyCenter) {
		this.center = new StockWritable(canopyCenter.center);
		this.closeVectors = new IntWritable(0);
		addConnectedStocks(canopyCenter.closeVectors.get());
	}

	public ClusterCenter(StockWritable stock) {
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

	public boolean converged(ClusterCenter c) {
		return compareTo(c) == 0 ? false : true;
	}

	@Override
	public int compareTo(ClusterCenter o) {
		return this.center.compareTo(o.center);
	}

	public boolean equals(Object o) {
		if (o instanceof ClusterCenter) {
			ClusterCenter other = (ClusterCenter) o;
			return this.center.equals(other.getCenter())
					&& this.closeVectors.equals(other.getCloseVectors());
		}
		return false;
	}

	@Override
	public int hashCode() {
		return this.closeVectors.hashCode() * this.center.hashCode();
	}

	@Override
	public String toString() {
		return "ClusterCenter:[\ncenter=" + this.center + "]";
	}
}
