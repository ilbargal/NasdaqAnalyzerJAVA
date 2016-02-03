package Writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;

public class Vector implements WritableComparable<Vector> {

	public enum VectorType {
		  OPEN("OPEN"),
		  HIGH("HIGH"),
		  LOW("LOW"),
		  CLOSE("CLOSE");
		  
		  @SuppressWarnings("unused")
		  private final String value;
		  
		  private VectorType(String value)
		  {
			  this.value = value;
		  }
	  }
	 
	private double[] vector;
	
	public Vector() {
		super();
	}
	

	public Vector(Vector v) {
		super();
		int l = v.vector.length;
		this.vector = new double[l];
		System.arraycopy(v.vector, 0, this.vector, 0, l);
	}

	public Vector(double[] values) {
		super();
		this.vector = values;
	}
	
	public void set(double[] values) {
		this.vector = values;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(vector.length);
		for (int i = 0; i < vector.length; i++)
			out.writeDouble(vector[i]);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		vector = new double[size];
		for (int i = 0; i < size; i++)
			vector[i] = in.readDouble();
	}

	@Override
	public int compareTo(Vector o) {
		for (int i = 0; i < vector.length; i++) {
			double c = vector[i] - o.vector[i];
			if (c!= 0.0d)
			{
				return (int)c;
			}		
		}
		return 0;
	}
	
	public double[] getVector() {
		return this.vector;
	}

	public void setVector(double[] vector) {
		this.vector = vector;
	}

	@Override
	public String toString() {
		return "Vector: " + Arrays.toString(this.vector);
	}
}
