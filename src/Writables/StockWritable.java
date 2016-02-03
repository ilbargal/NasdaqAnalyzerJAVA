package Writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import Writables.Vector.VectorType;

public class StockWritable implements WritableComparable<StockWritable> {

	Text name;
	Vector highVector;
	Vector lowVector;
	Vector openVector;
	Vector closeVector;
	
	public StockWritable() {
		super();
		this.name = new Text();
		highVector = new Vector();
		openVector = new Vector();
		lowVector = new Vector();
		closeVector = new Vector();
	}
	
	public StockWritable(Text stockName) {
		super();
		this.name = new Text(stockName);
		highVector = new Vector();
		openVector = new Vector();
		lowVector = new Vector();
		closeVector = new Vector();
	}
	
	public StockWritable(StockWritable stock) {
		this.name = new Text(stock.name);
		this.highVector = new Vector(stock.highVector);
		this.lowVector = new Vector(stock.lowVector);
		this.openVector = new Vector(stock.openVector);
		this.closeVector = new Vector(stock.closeVector);
	}
	
	public void setStockName(String stockName) {
		this.name = new Text(stockName);
	}
	
	public String getStockName() {
		return this.name.toString();
	}
	
	public int getStockDays() {
		return this.highVector.getVector().length;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.name.readFields(in);
		this.openVector.readFields(in);
		this.highVector.readFields(in);
		this.lowVector.readFields(in);
		this.closeVector.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.name.write(out);
		this.openVector.write(out);
		this.highVector.write(out);
		this.lowVector.write(out);
		this.closeVector.write(out);
	}

	@Override
	public int compareTo(StockWritable o) {
		return this.name.compareTo(o.name);
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof StockWritable){
			StockWritable other = (StockWritable)o;
			return this.name.equals(other.name);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return this.name.hashCode() +
				this.highVector.hashCode() * 2 +
				this.openVector.hashCode() +
				this.closeVector.hashCode() +
				this.lowVector.hashCode(); 
	}
	
	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append(name + "\n");
		result.append(openVector + " ");
		result.append(highVector + " ");
		result.append(lowVector + " ");
		result.append(closeVector + " ");
		return result.toString();
	}
	
	public Vector getStockVector(VectorType type) {
		switch (type) {
			case OPEN:
				return openVector;
			case HIGH:
				return highVector;
			case LOW:
				return lowVector;
			case CLOSE:
				return closeVector;
			default:
				return null;
		}
	}

}
