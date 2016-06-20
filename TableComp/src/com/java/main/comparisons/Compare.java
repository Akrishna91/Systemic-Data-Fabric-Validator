package com.java.main.comparisons;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;

import com.java.main.aggregation.AggregationStatisticsDF;
import com.java.main.applyrules.ApplyRule;
import com.java.main.beans.ColSummary;
import com.java.main.beans.FinalSummaryBean;
import com.java.main.beans.MisMatchedRows;
import com.java.main.beans.RulesComparaorResult;
import com.java.main.businessrules.DistinctValueBusinessRulesImpl;
import com.java.main.businessrules.PossibleValuesBusinessRules;
import com.java.main.context.GetJavaSparkContext;
import com.java.main.interfaces.IBusinessRule;
import com.java.main.profiling.Profiling;
import com.java.main.ui.ConfigurationDetailsBean;
import com.java.main.utils.MatchedRecords;
import com.java.main.utils.RDDUtils;

public class Compare {

	private static LinkedHashMap<String, LinkedHashSet<String>> possibleValues;
	private static ArrayList<String> distinctValueCols;
	private static LinkedHashMap<String, ColSummary> srcColSummary;

	public static FinalSummaryBean compareData(ConfigurationDetailsBean cdb)
			throws NoSuchAlgorithmException, IOException {
		String srcPath = cdb.getConnectionDetailsBean()
				.getSrcFileAbsolutePath();
		String destPath = cdb.getConnectionDetailsBean()
				.getDestFileAbsolutePath();
		boolean isPar = !cdb.getConnectionDetailsBean().isCsvSource();

		// get the dataframe from corresponding file
		DataFrame srcDataFrame = Reader.read(srcPath, isPar,
				cdb.getColumnNames());
		DataFrame destDataFrame = Reader.read(destPath, isPar,
				cdb.getColumnNames());
		ArrayList<String> selec = new ArrayList<String>();
		for (String col : srcDataFrame.columns()) {
			selec.add(col);
		}
		// aggregationStatisticsDF.getAggregationComparison(summary1, summary2);

		return compareDF(srcDataFrame, destDataFrame, cdb);

	}

	public static FinalSummaryBean compareDF(DataFrame srcDataFrame,
			DataFrame destDataFrame, ConfigurationDetailsBean cdb) {

		FinalSummaryBean finalSummaryBean = new FinalSummaryBean();
		ArrayList<String> colNames = new ArrayList<String>();
		StructField[] fields = srcDataFrame.schema().fields();
		for (StructField field : fields) {
			colNames.add(field.name());
		}

		if (cdb.isPossibleValueRule() || cdb.isUniquenessRule()) {
			generateRules(srcDataFrame, cdb.isPossibleValueRule(),
					cdb.isUniquenessRule());
			ApplyRule ar = new ApplyRule();

			if (cdb.isPossibleValueRule()) {
				LinkedHashMap<String, RulesComparaorResult> possibleValueRuleResults = ar
						.possibleValueCompare(srcColSummary, destDataFrame);
				finalSummaryBean
						.setPossibleValueRuleResults(possibleValueRuleResults);
			}
			if (cdb.isUniquenessRule()) {

				LinkedHashMap<String, RulesComparaorResult> distinctRuleResults = ar
						.distinctValueCompare(distinctValueCols, destDataFrame);
				finalSummaryBean.setDistinctRuleResults(distinctRuleResults);
			}
		}

		if (cdb.isSummationRule()) {
			AggregationStatisticsDF aggregationStatisticsDF = new AggregationStatisticsDF();
			DataFrame summary1 = aggregationStatisticsDF
					.getAggregationStatistics(srcDataFrame);
			DataFrame summary2 = aggregationStatisticsDF
					.getAggregationStatistics(destDataFrame);
			finalSummaryBean.setSrcAggSummary(summary1);
			finalSummaryBean.setDestAggSummary(summary2);
		}
		// get the mismatched rows (src - dest)
		DataFrame srcExceptDest = srcDataFrame.except(destDataFrame);
		ArrayList<String> keyColumnNames = cdb.getKeyColumnNames();
		if (keyColumnNames == null || keyColumnNames.size() == 0) {
			List<Row> srcExceptDestRow = srcExceptDest.collectAsList();
			List<String> missingRows = new ArrayList<String>();
			for (Row row : srcExceptDestRow) {
				missingRows.add(row.toString());
			}
			finalSummaryBean.setMissingRows(missingRows);
		} else {
			MatchedRecords matchedRecords = new MatchedRecords(srcExceptDest,
					destDataFrame, cdb.getKeyColumnNames(),
					GetJavaSparkContext.getSqlContext());
			// srcExceptDest.show();
			srcExceptDest.cache();
			DataFrame misMatchedDest = matchedRecords.fetMatchingRecords(true);
			ArrayList<Integer> keyColIndex = new ArrayList<Integer>();
			for(String keyCol : cdb.getKeyColumnNames()){
				for(int i = 0; i< colNames.size(); i++){
					if(keyCol.equals(colNames.get(i))){
						keyColIndex.add(i);
						break;
					}
				}
			}
			finalSummaryBean.setKeyColIndex(keyColIndex);

			/*
			 * DataFrame lostMatchedDest = matchedRecords
			 * .fetMatchingRecords(false);
			 */

			MisMatchedRows misMatchedRows = RDDUtils.misMatchedRows(
					misMatchedDest.toJavaRDD(), colNames);
			JavaRDD<String> srcExceptDestStringRDD = RDDUtils
					.rddRowTORddString(srcExceptDest.toJavaRDD());
			List<String> lostRows = srcExceptDestStringRDD.subtract(
					misMatchedRows.getRddStringMisRows()).collect();
			finalSummaryBean.setMisMatchedRows(misMatchedRows
					.getMisMatchedRowList());
			finalSummaryBean.setMissingRows(lostRows);
		}
		return finalSummaryBean;

	}

	/**
	 * sets the column involved in particular type of rule
	 * 
	 * @param dataFrame
	 * @param headers
	 * @param schema
	 * @param isPossibleValue
	 * @param isUniqueValue
	 * @return hashmap of rule type and corresponding rules
	 * 
	 */
	private static HashMap<String, ArrayList<String>> generateRules(
			DataFrame dataFrame, boolean isPossibleValue, boolean isUniqueValue) {
		HashMap<String, ArrayList<String>> rules = new LinkedHashMap<String, ArrayList<String>>();
		// String[] colDataType = new String[headers.size()];

		Profiling profiling = new Profiling();
		// HashSet<String> colNames = new HashSet<String>(headers);

		LinkedHashMap<String, ColSummary> colSummary = profiling
				.profiling(dataFrame);

		setSrcColSummary(colSummary);
		/*
		 * for (String col : colSummary.keySet()) { System.out .println(col +
		 * " " + colSummary.get(col).getCardinality()); }
		 */

		ArrayList<IBusinessRule> ruleGenrators = new ArrayList<IBusinessRule>();
		IBusinessRule pbr = null;
		IBusinessRule dbr = null;
		if (isUniqueValue) {
			dbr = new DistinctValueBusinessRulesImpl();
			ruleGenrators.add(dbr);
		}
		if (isPossibleValue) {
			pbr = new PossibleValuesBusinessRules();
			ruleGenrators.add(pbr);
		}/*
		 * if (pbr != null) { setPossibleValues(((PossibleValuesBusinessRules)
		 * pbr) .getPossibleValues()); }
		 */
		if (dbr != null) {
			setDistinctValueCols(((DistinctValueBusinessRulesImpl) dbr)
					.getDistinctValueHolders());
		}
		return rules;
	}

	public LinkedHashMap<String, LinkedHashSet<String>> getPossibleValues() {
		return possibleValues;
	}

	public static void setPossibleValues(
			LinkedHashMap<String, LinkedHashSet<String>> possibleValues1) {
		possibleValues = possibleValues1;
	}

	public ArrayList<String> getDistinctValueCols() {
		return distinctValueCols;
	}

	public static void setDistinctValueCols(ArrayList<String> distinctValueCols1) {
		distinctValueCols = distinctValueCols1;
	}

	/**
	 * @return the srcColSummary
	 */
	public static LinkedHashMap<String, ColSummary> getSrcColSummary() {
		return srcColSummary;
	}

	/**
	 * @param srcColSummary
	 *            the srcColSummary to set
	 */
	public static void setSrcColSummary(
			LinkedHashMap<String, ColSummary> srcColSummary) {
		Compare.srcColSummary = srcColSummary;
	}

}
