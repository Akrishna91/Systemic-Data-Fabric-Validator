package com.java.main.processor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Scanner;

import javax.swing.JFileChooser;
import javax.swing.JOptionPane;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.java.main.beans.ColSummary;
import com.java.main.beans.FinalSummaryBean;
import com.java.main.beans.MisMatchedRow;
import com.java.main.beans.MisMatchedRows;
import com.java.main.beans.MisMatchedValues;
import com.java.main.beans.RulesComparaorResult;
import com.java.main.businessrules.DistinctValueBusinessRulesImpl;
import com.java.main.businessrules.PossibleValuesBusinessRules;
import com.java.main.comparisons.Compare;
import com.java.main.constants.AggregationFuncNames;
import com.java.main.constants.ValueSeparater;
import com.java.main.context.GetJavaSparkContext;
import com.java.main.interfaces.IBusinessRule;
import com.java.main.profiling.Profiling;
import com.java.main.ui.ConfigurationDetailsBean;
import com.java.main.ui.ConnectionDetailsBean;
import com.java.main.ui.ReportGenrator;
import com.java.main.utils.MatchedRecords;
import com.java.main.utils.RDDUtils;

public class Processor {

	public static void main(String[] args) throws NoSuchAlgorithmException,
			IOException {

		long startTime = System.currentTimeMillis();
		BufferedReader inp = new BufferedReader(
				new InputStreamReader(System.in));
		ConfigurationDetailsBean cdb = new ConfigurationDetailsBean();
		ConnectionDetailsBean connectionDetailsBean = new ConnectionDetailsBean();
		ArrayList<String> keyColName = new ArrayList<String>();
		System.out.print("Is the source parquete? (true/false): ");
		String isPar = inp.readLine().trim();
		if (isPar.equalsIgnoreCase("true")) {
			connectionDetailsBean.setCsvSource(false);
		} else {
			connectionDetailsBean.setCsvSource(true);
		}
		System.out.print("Enter Database1 Directory: ");
		connectionDetailsBean.setSrcFileAbsolutePath(inp.readLine().trim());
		System.out.print("Enter Database2 Directory: ");
		connectionDetailsBean.setDestFileAbsolutePath(inp.readLine().trim());
		System.out.print("Is there any key column? (true/false): ");
		String isKey = inp.readLine();
		if (isKey.equalsIgnoreCase("true")) {
			System.out
					.print("Enter key columns (comma separated if more than one): ");
			for (String col : inp.readLine().trim().split(",")) {
				keyColName.add(col.trim());
			}
			cdb.setKeyColumnNames(keyColName);
		}

		cdb.setConnectionDetailsBean(connectionDetailsBean);
		FinalSummaryBean finalSummaryBean = Compare.compareData(cdb);
		printResult(finalSummaryBean);

		long endTime = System.currentTimeMillis();
		System.out.println(endTime - startTime);
	}

	public static void driver(ConfigurationDetailsBean cdb)
			throws NoSuchAlgorithmException, IOException {

		
		// printResult(finalSummaryBean);

		JFileChooser chooser = new JFileChooser();
		chooser.setCurrentDirectory(new java.io.File("."));
		chooser.setDialogTitle("Select location to save file");
		chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		chooser.setAcceptAllFileFilterUsed(false);

		if (chooser.showOpenDialog(chooser) == JFileChooser.APPROVE_OPTION) {
			String folderPath = chooser.getSelectedFile().getAbsolutePath();
			
			long startTime = System.currentTimeMillis();
			FinalSummaryBean finalSummaryBean = Compare.compareData(cdb);

			try {
				ReportGenrator.genrateReport(cdb, finalSummaryBean, folderPath);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			long endTime = System.currentTimeMillis();
			System.out.println(endTime - startTime);
			
			

		} else {
			System.out.println("No Selection ");
		}
		System.exit(0);
		
	}

	/**
	 * 
	 * @param file
	 * @return colNames and their data types
	 * @throws FileNotFoundException
	 */
	public HashMap<String, String> getParqueteColumnsDetails(String file)
			throws FileNotFoundException {
		HashMap<String, String> colNamesTypes = new LinkedHashMap<String, String>();
		JavaSparkContext jsc = GetJavaSparkContext.getJavaSparkContex();
		org.apache.spark.sql.SQLContext sqlContext = new org.apache.spark.sql.SQLContext(
				jsc);
		StructField[] fields = sqlContext.parquetFile(file).schema().fields();

		for (StructField field : fields) {
			colNamesTypes.put(field.name(), field.dataType().toString());
		}

		return colNamesTypes;
	}

	/**
	 * 
	 * @param file
	 * @return colNames and their data types
	 * @throws FileNotFoundException
	 */
	public HashMap<String, String> getColumnsDetails(String file)
			throws FileNotFoundException {
		HashMap<String, String> colNamesTypes = new LinkedHashMap<String, String>();
		@SuppressWarnings("resource")
		Scanner input = new Scanner(new File(file));
		String[] colNames = input.nextLine().trim()
				.split(ValueSeparater.getRegex());
		ArrayList<DataType> dataType = RDDUtils.genrateDataTypes(input
				.nextLine());
		int numRowsTried = 0;
		for (int i = 0; i < colNames.length; i++) {
			// System.out.println(colNames[i].trim());
			String type = "";
			try {
				if (dataType.get(i).equals(DataTypes.StringType)) {
					type = DataTypes.StringType.simpleString();
				} else if (dataType.get(i).equals(DataTypes.DateType)) {
					type = "Date";
				} else {
					type = "Numeric";
				}
			} catch (IndexOutOfBoundsException e) {
				if (numRowsTried < 100) {
					dataType = RDDUtils.genrateDataTypes(input.nextLine());
					i--;
					numRowsTried++;
				} else {
					type = DataTypes.StringType.simpleString();
				}
			}

			colNamesTypes.put(colNames[i].trim(), type);
		}

		return colNamesTypes;
	}

	public static void printResult(FinalSummaryBean finalSummaryBean) {
		List<MisMatchedRow> misMatchedRows = finalSummaryBean
				.getMisMatchedRows();
		List<String> lostRows = finalSummaryBean.getMissingRows();
		if (misMatchedRows != null && misMatchedRows.size() > 0) {
			System.out
					.println("------------Mis Matched rows-----------------------");
			for (MisMatchedRow misMatchedRow : misMatchedRows) {
				System.out.println(misMatchedRow.getRow());
				for (MisMatchedValues matchedValues : misMatchedRow
						.getSrcDestMisValues()) {
					System.out.println("COL Name: "
							+ matchedValues.getColName());
					System.out.println("Src Val: " + matchedValues.getSrcVal()
							+ "\t" + "Dest Val: " + matchedValues.getDestVal());
				}
			}
		}

		if (lostRows != null && lostRows.size() > 0) {
			System.out.println("Lost Rows");
			for (String lostRow : lostRows) {
				System.out.println(lostRow);
			}
		}

		LinkedHashMap<String, RulesComparaorResult> distincResult = finalSummaryBean
				.getDistinctRuleResults();
		for (String col : distincResult.keySet()) {
			System.out
					.println(col + " : " + distincResult.get(col).getStatus());
		}

		LinkedHashMap<String, RulesComparaorResult> possResult = finalSummaryBean
				.getPossibleValueRuleResults();
		for (String col : possResult.keySet()) {
			System.out.println(col + " : " + possResult.get(col).getStatus()
					+ " : " + possResult.get(col).getDestValues() + " : "
					+ possResult.get(col).getSourceValues());
		}
		if (finalSummaryBean.isMatched()) {
			System.out.println("Data Matched!");
		}

	}

}
