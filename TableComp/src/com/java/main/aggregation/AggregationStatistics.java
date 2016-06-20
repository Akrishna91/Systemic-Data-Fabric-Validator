package com.java.main.aggregation;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.java.main.utils.RDDUtils;

import scala.Tuple2;

public class AggregationStatistics implements Serializable {

	/**
                * 
                 */
	private static final long serialVersionUID = -7338542570088012374L;
	transient private JavaSparkContext jsc;
	private String ipPath;

	public AggregationStatistics(JavaSparkContext jsc, String ipPath) {
		this.jsc = jsc;
		this.ipPath = ipPath;
	}

	public void getAggregationStatistics() {

		System.out.println(new Date());
		JavaRDD<String> data = jsc.textFile(ipPath);
		System.out.println(data.count());
		final String[] headers = data.first().split(",");
		JavaRDD<String> valuesWihtoutHeaders = RDDUtils.removeHeaders(data);
		final JavaPairRDD<String, String> aftGrouping = valuesWihtoutHeaders
				.map(new Function<String, String>() {

					/**
                                                                                * 
                                                                                 */
					private static final long serialVersionUID = 1L;

					@Override
					public String call(String arg) throws Exception {
						String rowValue = "";
						String[] row = arg.split(",");
						for (int i = 0; i < row.length; i++) {
							row[i] = headers[i] + "," + row[i];
							rowValue += row[i] + "|$";
						}
						// System.out.println(rowValue);
						return rowValue;
					}
				}).flatMap(new FlatMapFunction<String, String>() {
					/**
                                                                                * 
                                                                                 */
					private static final long serialVersionUID = 1L;

					public Iterable<String> call(String x) {
						return Arrays.asList(x.split("\\|\\$"));
					}
				}).mapToPair(new PairFunction<String, String, String>() {

					/**
                                                * 
                                                 */
					private static final long serialVersionUID = 3860669960653090605L;

					@Override
					public Tuple2<String, String> call(String arg0)
							throws Exception {
						String[] values = arg0.split(",", -1);
						return new Tuple2<String, String>(values[0], values[1]);
					}
				});
		JavaPairRDD<String, String> stat = aftGrouping
				.reduceByKey(new Function2<String, String, String>() {

					/**
                                                * 
                                                 */
					private static final long serialVersionUID = -7556968764316757097L;

					@Override
					public String call(String arg0, String arg1)
							throws Exception {
						String[] val0;
						String max = "", min = "";
						if (arg0.contains(",")) {
							val0 = arg0.split(",");
							if (Double.valueOf(arg1) >= Double.valueOf(val0[2])) {
								max = arg1;
								min = val0[3];
							} else if (Double.valueOf(val0[3]) >= Double
									.valueOf(arg1)) {
								max = val0[2];
								min = arg1;
							} else {
								max = val0[2];
								min = val0[3];
							}
							return String.valueOf(Double.valueOf(val0[0])
									+ Double.valueOf(arg1))
									+ ","
									+ String.valueOf((Double.valueOf(val0[1]) + Double
											.valueOf(1)))
									+ ","
									+ max
									+ ","
									+ min;
						} else {
							if (Double.valueOf(arg0) >= Double.valueOf(arg1)) {
								max = arg0;
								min = arg1;
							} else {
								max = arg1;
								min = arg0;
							}
						}
						return String.valueOf(Double.valueOf(arg0)
								+ Double.valueOf(arg1))
								+ ","
								+ String.valueOf((Double.valueOf(1)))
								+ "," + max + "," + min;
					}
				});
		System.out.println(stat.first()._1 + " "
				+ Double.valueOf(stat.first()._2.split(",")[0])
				/ (Double.valueOf(stat.first()._2.split(",")[1]) + 1));
		System.out.println(new Date());
	}

}
