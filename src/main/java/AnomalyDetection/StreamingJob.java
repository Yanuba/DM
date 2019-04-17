package AnomalyDetection;

import AnomalyDetection.DataTypes.OutSensorRecord;
import AnomalyDetection.DataTypes.SensorRecord;
import AnomalyDetection.DataTypes.WindowRecord;
import com.github.signaflo.math.operations.DoubleFunctions;
import com.github.signaflo.timeseries.TimeSeries;
import com.github.signaflo.timeseries.forecast.Forecast;
import com.github.signaflo.timeseries.model.arima.Arima;
import com.github.signaflo.timeseries.model.arima.ArimaOrder;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StreamingJob {

	public static void main(String[] args) throws Exception {

		final String Input;
		final String Output;

		final ParameterTool params = ParameterTool.fromArgs(args);
		Input = params.has("input") ? params.get("input") : "dataset/LoRa/dragino.data";
		Output = params.has("output") ? params.get("output") : "out/flink/LoRa";



		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		/*
		 * Read csv
		 * Convert row into records
		 * assign timestamp from record
		 * aggregate windows
		 * */
		KeyedStream<SensorRecord,String> sensorStream = env
				.readTextFile(Input)
				.map(new SensorRecordMapper())
				.assignTimestampsAndWatermarks(new TSExtractor())
				.keyBy((KeySelector<SensorRecord, String>) sensorRecord -> sensorRecord.f0);

		/*
		 * @TODO:
		 * Use a proper sink
		 * */
		DataStream<OutSensorRecord> markedStream = sensorStream
				.window(TumblingEventTimeWindows.of(Time.hours(1)))
				.process(new RecordProcessing());

		markedStream
				.writeAsText(Output + "/out.csv", FileSystem.WriteMode.OVERWRITE)
				.setParallelism(1);

		DataStream<WindowRecord> windowStream = markedStream
				.keyBy((KeySelector<OutSensorRecord, String>) sensorRecord -> sensorRecord.f0)
				.window(TumblingEventTimeWindows.of(Time.hours(1)))
				.aggregate(new ReduceToWindow(), new SummarizeWindow());

		windowStream
				.writeAsText(Output +"/win.csv", FileSystem.WriteMode.OVERWRITE)
				.setParallelism(1);
		/*
		 * NB the ARIMA implementation found, require to predict the whole time series in advance
		 * */

		env.execute("ArimaPredictionJob");

	}

	// used to map line->record
	private static class SensorRecordMapper implements MapFunction<String, SensorRecord> {
		@Override
		public SensorRecord map(String s) {
			return SensorRecord.fromString(s);
		}
	}

	// used to get ts from a record
	private static class TSExtractor extends AscendingTimestampExtractor<SensorRecord> {
		@Override
		public long extractAscendingTimestamp(SensorRecord record) {
			return record.f1;
		}
	}

	// usa a RichMap function
	private static class RecordProcessing extends ProcessWindowFunction<SensorRecord, OutSensorRecord, String, TimeWindow> {

		private ValueState<Arima> ModelState;
		private ListState<Double> TrainingState;
		//add two list states, one for previous, one for current

		@Override
		public void open(Configuration parameters) throws Exception {
			ValueStateDescriptor<Arima> descriptor = new ValueStateDescriptor<>(
					"Model",
					TypeInformation.of(new TypeHint<Arima>() {
					})
			);
			ModelState = getRuntimeContext().getState(descriptor);

			ListStateDescriptor<Double> descriptor2 = new ListStateDescriptor<Double>(
					"PastObservations",
					TypeInformation.of(new TypeHint<Double>() {
					})
			);
			TrainingState = getRuntimeContext().getListState(descriptor2);
		}

		@Override
		public void process(String key, Context ctx, Iterable<SensorRecord> iterable, Collector<OutSensorRecord> out) throws Exception {
			int size = 1000;

			String type = key.split("/")[1];
			double thresh;
			switch (type) {
				case "temp":
					thresh = 0.75;
					break;
				case "sound":
					thresh = 10;
					break;
				case "humid":
					thresh = 4;
					break;
				case "light":
					thresh = 400;
					break;
				case "pir":
					thresh = 0.5;
					break;
				case "rssi":
					thresh = 8;
					break;
				default:
					thresh = 0.5;
					break;
			}

			Arima model = ModelState.value();

			double[] preds;

			if (model == null) {
				//make void prediction
				preds = new double[size];
			} else {
				// make prediction
				Forecast predictions = model.forecast(size);
				preds = predictions.pointEstimates().asArray();
			}

			ArrayList<Double> values = new ArrayList<>();

			int i = 0;
			for (SensorRecord sens : iterable) {
				//check if value is outlier
				double err = sens.f2 - preds[i];
				err = err>0?err:err*-1;
				out.collect(new OutSensorRecord(sens.f0, sens.f1, sens.f2, preds[i], err>thresh));
				values.add(sens.f2);

				i += 1;
			}

			// extract last x observations for training
			Iterator<Double> PastIterator = TrainingState.get().iterator();

			List<Double> toTrain = new ArrayList<>(IteratorUtils.toList(PastIterator));
			toTrain.addAll(values);

			//get last 500 elements
			if (toTrain.size() <= 500) {
				TrainingState.addAll(toTrain);
				return;
			}
			else {
				toTrain = toTrain.subList(toTrain.size()-500,toTrain.size());
				TrainingState.update(toTrain);
			}

			TimeSeries ts = TimeSeries.from(DoubleFunctions.arrayFrom(values));

			/*
			 * @TODO: find best order - is there a more efficient way?
			 * (Except finding the model before - must be done fore every sensor)
			 * */
			ArimaOrder bestOrder = ArimaOrder.order(0,1,1); //exponential smoothing
			Arima bestModel = Arima.model(ts, bestOrder);


			/*
			// small grid search - Too slow
			double bestAic = bestModel.aic();

			ArimaOrder tmpOrder;
			Arima tmpModel;
			double tmpAic;

			for (int p = 0; p<=5; p++) {
				for (int d = 0; d<3; d++) {
					for (int q = 0; q<=5; q++) {
						tmpOrder = ArimaOrder.order(p,d,q);
						tmpModel = Arima.model(ts, tmpOrder);
						tmpAic = tmpModel.aic();

						if (tmpAic < bestAic) {
							bestModel = tmpModel;
							bestAic = tmpAic;
						}
					}
				}
			}
			*/

			ModelState.update(bestModel);

		}
	}

	private static class ReduceToWindow implements AggregateFunction<OutSensorRecord, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		//f0 - inliers
		//f1 - outliers

		@Override
		public Tuple2<Integer, Integer> createAccumulator() {
			return new Tuple2<>(0,0);
		}

		@Override
		public Tuple2<Integer, Integer> add(OutSensorRecord sr, Tuple2<Integer, Integer> acc) {
			Boolean isOut = sr.f4;
			if (isOut) {
				return new Tuple2<>(acc.f0, acc.f1 + 1);
			}
			else {
				return new Tuple2<>(acc.f0 + 1, acc.f1);
			}
		}

		@Override
		public Tuple2<Integer, Integer> getResult(Tuple2<Integer, Integer> acc) {
			return acc;
		}

		@Override
		public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a0, Tuple2<Integer, Integer> a1) {
			return new Tuple2<>(a0.f0 + a1.f0, a0.f1 + a1.f1);
		}
	}

	private static class SummarizeWindow extends ProcessWindowFunction<Tuple2<Integer, Integer>, WindowRecord, String, TimeWindow> {
		@Override
		public void process(String key, Context ctx, Iterable<Tuple2<Integer, Integer>> iterable, Collector<WindowRecord> out) throws Exception {
			//iterate over window
			Tuple2<Integer, Integer> res = iterable.iterator().next();
			out.collect(new WindowRecord(key, ctx.window().getStart(), ctx.window().getEnd(), res.f1, res.f0));
		}
	}

}
