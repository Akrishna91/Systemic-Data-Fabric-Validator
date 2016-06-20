package com.java.main.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import scala.Tuple2;

import com.java.main.beans.MisMatchedRow;
import com.java.main.beans.MisMatchedRows;
import com.java.main.beans.MisMatchedValues;
import com.java.main.constants.ValueSeparater;
import com.java.main.context.GetJavaSparkContext;

public class RDDUtils {

	public static Function<Tuple2<String, String>, Boolean> possibleValuesLessThan10 = new Function<Tuple2<String, String>, Boolean>() {
		/**
				 * 
				 */
		private static final long serialVersionUID = -5028052708324347182L;

		public Boolean call(Tuple2<String, String> keyValue) {
			return (keyValue._2().length() > 10);
		}
	};

	/**
	 * Returns the object function to remove the header from a RDD
	 * 
	 * @param srcFile
	 * @return file RDD without headers
	 */
	public static JavaRDD<String> removeHeaders(JavaRDD<String> srcFile) {
		Function2<Integer, Iterator<String>, Iterator<String>> rowsWithoutHeader = new Function2<Integer, Iterator<String>, Iterator<String>>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = -6206662062838861709L;

			@Override
			public Iterator<String> call(Integer ind, Iterator<String> iterator)
					throws Exception {
				if (ind == 0 && iterator.hasNext()) {
					iterator.next();
					return iterator;
				} else
					return iterator;
			}
		};

		return srcFile.mapPartitionsWithIndex(rowsWithoutHeader, false);
	}

	/**
	 * 
	 * @param String
	 *            schemaString (column Names) comma saparated
	 * @param dataType
	 *            (array of datatypes)
	 * @return
	 */
	public static List<StructField> genrateSchema(String[] schemaString,
			ArrayList<DataType> dataType) {
		List<StructField> fields = new ArrayList<StructField>();
		int i = 0;
		//System.out.println(dataType);
		for (String fieldName : schemaString) {
			fields.add(DataTypes.createStructField(fieldName.trim(),
					dataType.get(i), true));
			i++;
		}

		return fields;

	}

	/**
	 * 
	 * @param sample
	 * @param fields
	 * @return
	 */
	public static JavaRDD<Row> genrateJavaRDD(JavaRDD<String> sample,
			final List<StructField> fields) {
		final HashMap<String, String> dateFormates = getDateFormate(fields,
				sample.first().trim().split(","));
		JavaRDD<Row> rowRDD = sample.map(new Function<String, Row>() {
			/**
					 * 
					 */
			private static final long serialVersionUID = 1L;

			public Row call(String record) throws Exception {
				String[] lineString = record.split(ValueSeparater.getRegex());
				Object[] cells = new Object[fields.size()];
				for (int i = 0; i < fields.size(); i++) {
					// System.out.println(fields.get(i));
					if (fields.get(i).dataType().equals(DataTypes.DoubleType)) {
						try {
							cells[i] = Double.parseDouble(lineString[i].trim());
						} catch (Exception e) {
							cells[i] = Double.parseDouble("0");
						}
					} else if (fields.get(i).dataType()
							.equals(DataTypes.DateType)) {
						try {
							String fieldName = fields.get(i).name();
							cells[i] = new java.sql.Date(DateUtils.parse(
									lineString[i].trim(),
									dateFormates.get(fieldName)).getTime());
						} catch (Exception e) {
							cells[i] = new java.sql.Date(DateUtils.parse(
									"01-01-0001").getTime());
						}
					} else {

						try {
							if (lineString[i].trim().length() != 0) {
								cells[i] = lineString[i].trim();
							} else {
								cells[i] = "Na";
							}
						} catch (Exception e) {
							cells[i] = "Na";
						}
					}
				}
				return RowFactory.create(cells);
			}
		});

		return rowRDD;

	}

	/**
	 * To generate DataTypes using the first row of the data
	 * 
	 * @param firstRow
	 *            (CSV String value)
	 * @return
	 */
	public static ArrayList<DataType> genrateDataTypes(String firstRow) {

		ArrayList<DataType> dataType = new ArrayList<DataType>();
		String[] firstRowArray = firstRow.trim().split(
				ValueSeparater.getRegex(), -1);

		for (String cell : firstRowArray) {
			try {
				if (DataTypeUtils.isDouble(cell.trim())) {
					dataType.add(DataTypes.DoubleType);
				} else if (DateUtils.isValidDate(cell.trim())) {
					dataType.add(DataTypes.DateType);
				} else {
					dataType.add(DataTypes.StringType);
				}
			} catch (NullPointerException ex) {
				// if null then keep the data type as string
				dataType.add(DataTypes.StringType);
				System.out.println(" Null found in resolving data type");
			}

		}
		return dataType;
	}

	/**
	 * 
	 * @param fields
	 * @param data
	 *            (any non null row)
	 * @return colname and its date format as hashmap
	 */
	public static HashMap<String, String> getDateFormate(
			final List<StructField> fields, String[] data) {

		HashMap<String, String> dateFormates = new HashMap<>();
		for (int i = 0; i < fields.size(); i++) {
			if (fields.get(i).dataType().equals(DataTypes.DateType)) {
				try {
					String dateFormate = DateUtils.determineDateFormat(data[i]);
					dateFormates.put(fields.get(i).name(), dateFormate);

				} catch (Exception e) {
					e.printStackTrace();
				}

			}
		}
		return dateFormates;
	}

	public static MisMatchedRows misMatchedRows(JavaRDD<Row> src_dest_merged,
			ArrayList<String> colNames) {
		MisMatchedRows misMatchedRows = new MisMatchedRows();
		ArrayList<ArrayList<String>> meargedSrcRows = new ArrayList<ArrayList<String>>();
		List<MisMatchedRow> misMatchedRowList = src_dest_merged.map(
				new Function<Row, MisMatchedRow>() {
					/**
	 * 
	 */
					private static final long serialVersionUID = 1L;

					public MisMatchedRow call(Row row) {
						MisMatchedRow row_col_mis = new MisMatchedRow();
						ArrayList<String> currentRow = new ArrayList<String>();
						for (int i = 0; i < row.size() / 2; i++) {
							currentRow.add(isDateNull(row.getAs(i).toString()));
							if (!row.getAs(i).equals(
									row.getAs(i + colNames.size()))) {
								ArrayList<String> misMatchedCols = row_col_mis
										.getMisMatchedCols();
								ArrayList<MisMatchedValues> misVals = row_col_mis
										.getSrcDestMisValues();
								MisMatchedValues mv = new MisMatchedValues();
								misMatchedCols.add(colNames.get(i));
								mv.setColName(colNames.get(i));
								mv.setSrcVal(isDateNull(row.getAs(i).toString()));

								mv.setDestVal(isDateNull(row.getAs(
										i + colNames.size()).toString()));
								misVals.add(mv);
								row_col_mis.setMisMatchedCols(misMatchedCols);
							}
							meargedSrcRows.add(currentRow);
							row_col_mis.setRow(currentRow);
						}
						return row_col_mis;
					}
				}).collect();
		JavaRDD<String> misMatchedRowsRDD = src_dest_merged
				.map(new Function<Row, String>() {
					/**
	 * 
	 */
					private static final long serialVersionUID = 1L;

					public String call(Row row) {
						ArrayList<String> currentRow = new ArrayList<String>();
						for (int i = 0; i < row.size() / 2; i++) {
							currentRow.add(isDateNull(row.getAs(i).toString()));

						}
						return currentRow.toString();
					}
				});
		misMatchedRows.setMisMatchedRowList(misMatchedRowList);
		misMatchedRows.setRddStringMisRows(misMatchedRowsRDD);
		return misMatchedRows;

	}

	public static JavaRDD<String> listOfListToRDD(
			ArrayList<ArrayList<String>> rows) {
		JavaSparkContext jsc = GetJavaSparkContext.getJavaSparkContex();
		JavaRDD<ArrayList<String>> rddList = jsc.parallelize(rows);
		JavaRDD<String> rddString = rddList
				.map(new Function<ArrayList<String>, String>() {
					/**
* 
*/
					private static final long serialVersionUID = 1L;

					public String call(ArrayList<String> row) {
						return row.toString();
					}
				});
		return rddString;
	}

	public static JavaRDD<String> rddRowTORddString(JavaRDD<Row> rows) {
		JavaRDD<String> rddString = rows.map(new Function<Row, String>() {
			/**
* 
*/
			private static final long serialVersionUID = 1L;

			public String call(Row row) {
				ArrayList<String> rowList = new ArrayList<String>();
				for (int i = 0; i < row.size(); i++) {
					rowList.add(row.getAs(i).toString());
				}
				return rowList.toString();
			}
		});
		return rddString;
	}

	public static String isDateNull(String val) {
		if (val.equals("0001-01-01")) {
			return "null";
		} else {
			return val;
		}
	}

}
