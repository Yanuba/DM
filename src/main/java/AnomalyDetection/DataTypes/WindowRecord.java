package AnomalyDetection.DataTypes;

import org.apache.flink.api.java.tuple.Tuple5;

public class WindowRecord extends Tuple5<String, Long, Long, Integer, Integer> {
	public WindowRecord() {
	}

	public WindowRecord(String value0, Long value1, Long value2, Integer value3, Integer value4) {
		super(value0, value1, value2, value3, value4);
	}

	@Override
	public String toString() {
		return f0 + "," + f1 + "," + f2 + "," + f3 + "," + f4;
	}
}
