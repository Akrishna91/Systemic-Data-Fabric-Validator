package com.java.main.beans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

public class FinalSummaryBean {
	
	
	
	private boolean matched = true;
	private HashMap<String, ColResultSummaryBean> colResultSummaryBean;
	private HashMap<String, List<AggregationMismatches>> colNames_AggError;
	private List<String> missingRows;
	private LinkedHashMap<String, RulesComparaorResult> distinctRuleResults;
	private LinkedHashMap<String, RulesComparaorResult> possibleValueRuleResults;
	private List<MisMatchedRow> misMatchedRows;
	private ArrayList<Integer> keyColIndex;
	private DataFrame srcAggSummary;
	private DataFrame destAggSummary;
	
	/**
	 * @return the matched
	 */
	public boolean isMatched() {
		return matched;
	}
	/**
	 * @param matched the matched to set
	 */
	public void setMatched(boolean matched) {
		this.matched = matched;
	}
	public HashMap<String, ColResultSummaryBean> getColResultSummaryBean() {
		return colResultSummaryBean;
	}
	public void setColResultSummaryBean(HashMap<String, ColResultSummaryBean> colResultSummaryBean) {
		this.colResultSummaryBean = colResultSummaryBean;
	}
	public HashMap<String, List<AggregationMismatches>> getColNames_AggError() {
		return colNames_AggError;
	}
	public void setColNames_AggError(
			HashMap<String, List<AggregationMismatches>> colNames_AggError) {
		this.colNames_AggError = colNames_AggError;
	}
	public List<String> getMissingRows() {
		return missingRows;
	}
	public void setMissingRows(List<String> missingRows) {
		this.missingRows = missingRows;
		if (missingRows != null && missingRows.size() > 0){
			this.matched = false;
		}
	}
	public LinkedHashMap<String, RulesComparaorResult> getDistinctRuleResults() {
		return distinctRuleResults;
	}
	public void setDistinctRuleResults(
			LinkedHashMap<String, RulesComparaorResult> distinctRuleResults) {
		this.distinctRuleResults = distinctRuleResults;
	}
	public LinkedHashMap<String, RulesComparaorResult> getPossibleValueRuleResults() {
		return possibleValueRuleResults;
	}
	public void setPossibleValueRuleResults(
			LinkedHashMap<String, RulesComparaorResult> possibleValueRuleResults) {
		this.possibleValueRuleResults = possibleValueRuleResults;
	}
	/**
	 * @return the misMatchedRows
	 */
	public List<MisMatchedRow> getMisMatchedRows() {
		return misMatchedRows;
	}
	/**
	 * @param misMatchedRows the misMatchedRows to set
	 */
	public void setMisMatchedRows(List<MisMatchedRow> misMatchedRows) {
		this.misMatchedRows = misMatchedRows;
		if (misMatchedRows != null && misMatchedRows.size() > 0){
			this.matched = false;
		}
	}
	/**
	 * @return the srcAggSummary
	 */
	public DataFrame getSrcAggSummary() {
		return srcAggSummary;
	}
	/**
	 * @param srcAggSummary the srcAggSummary to set
	 */
	public void setSrcAggSummary(DataFrame srcAggSummary) {
		this.srcAggSummary = srcAggSummary;
	}
	/**
	 * @return the destAggSummary
	 */
	public DataFrame getDestAggSummary() {
		return destAggSummary;
	}
	/**
	 * @param destAggSummary the destAggSummary to set
	 */
	public void setDestAggSummary(DataFrame destAggSummary) {
		this.destAggSummary = destAggSummary;
	}
	/**
	 * @return the keyColIndex
	 */
	public ArrayList<Integer> getKeyColIndex() {
		return keyColIndex;
	}
	/**
	 * @param keyColIndex the keyColIndex to set
	 */
	public void setKeyColIndex(ArrayList<Integer> keyColIndex) {
		this.keyColIndex = keyColIndex;
	}

}
