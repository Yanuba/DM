package AnomalyDetection.DataTypes;

import org.apache.flink.api.java.tuple.Tuple3;

public class SensorRecord extends Tuple3<String, Long, Double> {
	public SensorRecord() {
	}

	public SensorRecord(String value0, Long value1, Double value2) {
		super(value0, value1, value2);
	}

	public static SensorRecord fromString(String s) {
		String[] tokens = s.split(",");

		String type = tokens[0];
		long ts = Long.parseLong(tokens[1]);
		double value = Double.parseDouble(tokens[2]);

		return new SensorRecord(type, ts, value);
	}

	@Override
	public String toString() {
		return  f0 + ',' + f1 + ',' + f2;
	}
}
