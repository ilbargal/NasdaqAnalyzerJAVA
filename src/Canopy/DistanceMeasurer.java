package Canopy;

import Writables.StockWritable;
import Writables.Vector.VectorType;

public class DistanceMeasurer {
	public static final double T1 = 3000;
	public static final double T2 = 0;
	public static final double measureDistance(StockWritable center, StockWritable stockVector) {
		double result = 0;
		
		for (int day = 0; day < center.getStockDays(); day++) {
			result += Math.abs(center.getStockVector(VectorType.OPEN).getVector()[day] - stockVector.getStockVector(VectorType.OPEN).getVector()[day]);
			result += Math.abs(center.getStockVector(VectorType.HIGH).getVector()[day] - stockVector.getStockVector(VectorType.HIGH).getVector()[day]);
			result += Math.abs(center.getStockVector(VectorType.LOW).getVector()[day] - stockVector.getStockVector(VectorType.LOW).getVector()[day]);
			result += Math.abs(center.getStockVector(VectorType.CLOSE).getVector()[day] - stockVector.getStockVector(VectorType.CLOSE).getVector()[day]);
		}
			
		return result;
	}
}
