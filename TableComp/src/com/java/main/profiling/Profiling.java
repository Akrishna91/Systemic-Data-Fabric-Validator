package com.java.main.profiling;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import scala.Tuple2;

import com.java.main.beans.ColSummary;
import com.java.main.constants.ValueSeparater;
import com.java.main.utils.DataFrameUtils;
import com.java.main.utils.RDDUtils;

import static org.apache.spark.sql.functions.*;

public class Profiling implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3306435084942735107L;

	public LinkedHashMap<String, ColSummary> profiling(DataFrame dataFrame,
			List<StructField> srcSchema, ArrayList<String> selectedCols) {
		// dataFrame.describe().show();
		System.out.println("Profiling Started");
		LinkedHashMap<String, ColSummary> colSummary = new LinkedHashMap<String, ColSummary>();
		HashSet<String> selectedColSet = new HashSet<String>();
		for (String selectedCol : selectedCols) {
			selectedColSet.add(selectedCol);
		}
		for (StructField field : srcSchema) {
			ColSummary currentColSummary = new ColSummary();
			String colName = field.name().trim();
			if (selectedColSet.contains(colName.trim())) {
				System.out.println(colName);
				LinkedHashSet<String> levels = null;
				long cardinality = DataFrameUtils.getCardinality(dataFrame,
						colName);
				currentColSummary.setCardinality(cardinality);
				if (cardinality <= 15) {
					levels = DataFrameUtils.getDistinctValues(dataFrame,
							colName);
					currentColSummary.setLevels(levels);
				}

				if (field.dataType().equals(DataTypes.StringType)) {
					currentColSummary.setType("String");
				} else if (field.dataType().equals(DataTypes.DateType)) {
					currentColSummary.setType("Date");
					currentColSummary.setFormats(DataFrameUtils.getDateFormats(
							dataFrame, colName));

				} else {
					currentColSummary.setType("Numeric");
					if (levels != null) {
						currentColSummary.setMax(Double.parseDouble(Collections
								.max(levels)));
						currentColSummary.setMin(Double.parseDouble(Collections
								.min(levels)));

					} else {
						Row[] max_min = dataFrame.select(max(colName),
								min(colName)).collect();
						currentColSummary.setMax(Double.parseDouble(max_min[0]
								.getAs(0).toString()));
						currentColSummary.setMin(Double.parseDouble(max_min[0]
								.getAs(1).toString()));
					}
				}
				currentColSummary.setLevels(levels);
				colSummary.put(colName, currentColSummary);
			}

		}
		return colSummary;

	}

	public LinkedHashMap<String, ColSummary> profiling(DataFrame dataFrame,
			List<StructField> srcSchema) {
		// dataFrame.describe().show();
		System.out.println("Profiling Started");
		LinkedHashMap<String, ColSummary> colSummary = new LinkedHashMap<String, ColSummary>();
		for (StructField field : srcSchema) {
			ColSummary currentColSummary = new ColSummary();
			String colName = field.name();
			System.out.println(colName);
			LinkedHashSet<String> levels = null;
			long cardinality = DataFrameUtils
					.getCardinality(dataFrame, colName);
			currentColSummary.setCardinality(cardinality);
			if (cardinality <= 15) {
				levels = DataFrameUtils.getDistinctValues(dataFrame, colName);
				currentColSummary.setLevels(levels);
			}

			if (field.dataType().equals(DataTypes.StringType)) {
				currentColSummary.setType("String");
			} else {
				currentColSummary.setType("Numeric");
				if (levels != null) {
					currentColSummary.setMax(Double.parseDouble(Collections
							.max(levels)));
					currentColSummary.setMin(Double.parseDouble(Collections
							.min(levels)));

				} else {
					Row[] max_min = dataFrame
							.select(max(colName), min(colName)).collect();
					currentColSummary.setMax(Double.parseDouble(max_min[0]
							.getAs(0).toString()));
					currentColSummary.setMin(Double.parseDouble(max_min[0]
							.getAs(1).toString()));
				}
			}
			currentColSummary.setLevels(levels);
			colSummary.put(colName, currentColSummary);
		}
		return colSummary;

	}

	public LinkedHashMap<String, ColSummary> profiling(
			JavaRDD<Row> valuesWihtoutHeaders, String[] headers) {

		LinkedHashMap<String, ColSummary> colSummary = new LinkedHashMap<String, ColSummary>();

		JavaRDD<String> aftGrouping = valuesWihtoutHeaders
				.map(new Function<Row, String>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public String call(Row arg) throws Exception {
						String rowValue = "";
						String[] row = new String[arg.size()];
						for (int i = 0; i < row.length; i++) {
							try {
								row[i] = headers[i] + ValueSeparater.getRegex()
										+ arg.get(i).toString();
							} catch (NullPointerException e) {
								row[i] = headers[i] + ValueSeparater.getRegex()
										+ "null";
							}
							rowValue += row[i]
									+ ValueSeparater.getRegexForCardinality();

						}
						return rowValue;
					}
				});
		JavaRDD<String> words = aftGrouping
				.flatMap(new FlatMapFunction<String, String>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Iterable<String> call(String x) {
						return Arrays.asList(x.split(ValueSeparater
								.getRegexForCardinalitytoSplit()));
					}
				});
		JavaPairRDD<String, String> pairs = words
				.mapToPair(new PairFunction<String, String, String>() {

					/**
			 * 
			 */
					private static final long serialVersionUID = 3860669960653090605L;

					@Override
					public Tuple2<String, String> call(String arg0)
							throws Exception {
						if (arg0.trim().length() > 0) {
							String[] values = arg0.split(
									ValueSeparater.getRegex(), -1);
							try {
								return new Tuple2<String, String>(values[0],
										values[1]);
							} catch (Exception e) {
								System.out.println("values0 " + values[0]);
								e.printStackTrace();
							}

						}
						return new Tuple2<String, String>(ValueSeparater
								.getRegex(), ValueSeparater.getRegex());
					}
				});
		// System.out.println("pairs count-->"+pairs.count());
		/*JavaPairRDD<String, String> counts = pairs.reduceByKey(new Function2<String, String, String>() {
			
			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String arg0, String arg1) throws Exception {
				try{
					int a = Integer.parseInt(arg0);
					return String.valueOf(new Integer(1) + a);
				}
				catch(Exception e){
					return String.valueOf(new Integer(2));
				}
			}
		});*/
		JavaPairRDD<String, Object> counts = pairs
				.countApproxDistinctByKey(0.01);

		for (Tuple2<String, Object> count : counts.collect()) {
			ColSummary currentColSummary = new ColSummary();
			currentColSummary.setCardinality((Long) count._2);
			LinkedHashSet<String> levels = new LinkedHashSet<String>();
			levels.add(count._2.toString() + " " + "levels");
			currentColSummary.setLevels(levels);
			colSummary.put(count._1, currentColSummary);
		}

		return colSummary;
	}

	public LinkedHashMap<String, ColSummary> profiling(DataFrame df) {
		return profiling(df.toJavaRDD(), df.columns());
	}
}
