package com.java.main.applyrules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.java.main.beans.ColSummary;
import com.java.main.beans.RulesComparaorResult;
import com.java.main.businessrules.DistinctValueBusinessRulesImpl;
import com.java.main.constants.RulesMatchingStatus;
import com.java.main.profiling.Profiling;
import com.java.main.utils.DataFrameUtils;

public class ApplyRule {

	public LinkedHashMap<String, RulesComparaorResult> possibleValueCompare(
			LinkedHashMap<String, ColSummary> srcColSummary,
			DataFrame destDataFrame) {
		LinkedHashMap<String, RulesComparaorResult> result = new LinkedHashMap<String, RulesComparaorResult>();
		Profiling profiling = new Profiling();
		LinkedHashMap<String, ColSummary> destColSummary = profiling.profiling(destDataFrame);
		
		for(String colName : destColSummary.keySet()){
			Long srcCardinality = destColSummary.get(colName).getCardinality();
			Long destCardinality = srcColSummary.get(colName).getCardinality();
			RulesComparaorResult rcr = new RulesComparaorResult();
			rcr.setDestValues(destColSummary.get(colName).getLevels());
			rcr.setSourceValues(srcColSummary.get(colName).getLevels());
			if (srcCardinality.equals(destCardinality)){
				rcr.setStatus(RulesMatchingStatus.MATCHED);
			}else if (srcCardinality < destCardinality){
				rcr.setStatus(RulesMatchingStatus.MIGHT_MATCH);
			}else{
				rcr.setStatus(RulesMatchingStatus.MISMATCHED);
			}
			result.put(colName, rcr);
		}
		return result;
	}

	public LinkedHashMap<String, RulesComparaorResult> distinctValueCompare(
			ArrayList<String> distinctValueCols, DataFrame destDataFrame) {
		LinkedHashMap<String, RulesComparaorResult> result = new LinkedHashMap<String, RulesComparaorResult>();
		if(distinctValueCols == null || distinctValueCols.size() == 0){
			return result;
		}
		DistinctValueBusinessRulesImpl distinctValueBusinessRulesImpl = new DistinctValueBusinessRulesImpl();
		DataFrame filteredDestDF = DataFrameUtils.select(destDataFrame,
				distinctValueCols);
		Profiling profiling = new Profiling();

		LinkedHashMap<String, ColSummary> destColSummary = profiling
				.profiling(filteredDestDF);
		distinctValueBusinessRulesImpl.genrateRule(destColSummary,
				(int) destDataFrame.count());
		ArrayList<String> destDistinctValueCols = distinctValueBusinessRulesImpl
				.getDistinctValueHolders();
		HashSet<String> srcDistinctValueCols = new HashSet<String>(
				destDistinctValueCols);

		for (String col : destDistinctValueCols) {
			RulesComparaorResult rcr = new RulesComparaorResult();
			LinkedHashSet<String> sourceCol = new LinkedHashSet<String>();
			LinkedHashSet<String> destCol = new LinkedHashSet<String>();
			sourceCol.add("unique");
			rcr.setSourceValues(sourceCol);
			if (srcDistinctValueCols.contains(col)) {
				rcr.setStatus(RulesMatchingStatus.MATCHED);
				destCol.add("unique");
			} else {
				rcr.setStatus(RulesMatchingStatus.MISMATCHED);
				destCol.add("not Unique");
			}
			rcr.setDestValues(destCol);
			result.put(col, rcr);
		}
		return result;

	}

	public LinkedHashSet<String> getDistinctValues(DataFrame dataFrame,
			String colName) {

		Row[] distinctValuesRow = dataFrame.select(colName).distinct()
				.collect();

		LinkedHashSet<String> distingValues = new LinkedHashSet<String>();
		for (int i = 0; i < distinctValuesRow.length; i++) {
			distingValues.add(distinctValuesRow[i].getAs(i).toString()
					.toUpperCase());
		}
		return distingValues;

	}

	public boolean isUniqueColumn(DataFrame dataFrame, String colName) {
		Long distinctValuesCount = dataFrame.select(colName).distinct().count();
		Long numRows = dataFrame.count();
		return distinctValuesCount.equals(numRows);
	}

	public boolean isUniqueColumn(DataFrame dataFrame) {
		Long distinctValuesCount = dataFrame.distinct().count();
		Long numRows = dataFrame.count();
		return distinctValuesCount.equals(numRows);
	}

}
