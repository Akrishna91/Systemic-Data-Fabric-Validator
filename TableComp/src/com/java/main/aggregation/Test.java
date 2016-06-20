package com.java.main.aggregation;

import org.apache.spark.api.java.JavaSparkContext;

import com.java.main.context.GetJavaSparkContext;

public class Test {

	public static void main(String[] args) {
		AggregationStatistics aggregationStatistics;
		JavaSparkContext jsc = GetJavaSparkContext.getJavaSparkContex();
		String ipPath = "/home/cloudera/Desktop/sampleCSV";
		aggregationStatistics = new AggregationStatistics(jsc, ipPath);
		aggregationStatistics.getAggregationStatistics();

	}

}
