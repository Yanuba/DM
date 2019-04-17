package AnomalyDetection.DataTypes;


import org.apache.flink.api.java.tuple.Tuple5;

public class OutSensorRecord extends Tuple5<String, Long, Double, Double, Boolean> {
	// True means Outlier
	// False means Inlier
	public OutSensorRecord() {
	}

	public OutSensorRecord(String value0, Long value1, Double value2, Double value3, Boolean value4) {
		super(value0, value1, value2, value3, value4);
	}

	@Override
	public String toString() {
		return f0 + "," + f1 + "," + f2 + "," + f3 + "," + f4;
	}
}
