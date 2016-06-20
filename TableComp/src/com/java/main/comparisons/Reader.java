package com.java.main.comparisons;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.java.main.constants.ValueSeparater;
import com.java.main.context.GetJavaSparkContext;
import com.java.main.utils.DataFrameUtils;
import com.java.main.utils.RDDUtils;

public class Reader {
	
	
	/**
	 * 
	 * @param dir
	 * @param isPar
	 * @return dataframe of the file
	 */
	public static DataFrame read(String dir, boolean isPar) {
		if (isPar) {
			return readParqeute(dir);
		} else {
			return readFile(dir);
		}
	}
	
	/**
	 * 
	 * @param dir
	 * @param isPar
	 * @return dataframe of the file
	 */
	public static DataFrame read(String dir, boolean isPar, ArrayList<String> selectedCols) {
		DataFrame df;
		if (isPar) {
			df = readParqeute(dir);
			
		} else {
			df =  readFile(dir);
		}
		return DataFrameUtils.select(df , selectedCols) ;
	}

	public static DataFrame readParqeute(String dir) {
		
		return GetJavaSparkContext.getSqlContext().parquetFile(dir);

	}

	public static DataFrame readFile(String dir) {
		JavaSparkContext jsc = GetJavaSparkContext.getJavaSparkContex();
		JavaRDD<String> file = jsc.textFile(dir);
		String[] headers = file.first().split(ValueSeparater.getRegex());
		JavaRDD<String> fileWithoutHeaders = RDDUtils.removeHeaders(file);
		ArrayList<DataType> dataType = RDDUtils
				.genrateDataTypes(fileWithoutHeaders.first());
		List<StructField> fields = RDDUtils.genrateSchema(headers,
				dataType);
		StructType schema = DataTypes.createStructType(fields);
		
		JavaRDD<Row> rowRDD = RDDUtils.genrateJavaRDD(
				fileWithoutHeaders, fields);
		DataFrame dataFrame = GetJavaSparkContext.getSqlContext().createDataFrame(rowRDD,
				schema);
		
		return dataFrame;

	}

}
