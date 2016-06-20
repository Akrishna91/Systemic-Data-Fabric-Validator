package com.java.main.beans;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;

public class MisMatchedRows {
	
	
	private List<MisMatchedRow> misMatchedRowList;
	private JavaRDD<String> rddStringMisRows;
	
	/**
	 * @return the rddStringMisRows
	 */
	public JavaRDD<String> getRddStringMisRows() {
		return rddStringMisRows;
	}
	/**
	 * @param rddStringMisRows the rddStringMisRows to set
	 */
	public void setRddStringMisRows(JavaRDD<String> rddStringMisRows) {
		this.rddStringMisRows = rddStringMisRows;
	}
	/**
	 * @return the misMatchedRowList
	 */
	public List<MisMatchedRow> getMisMatchedRowList() {
		return misMatchedRowList;
	}
	/**
	 * @param misMatchedRowList the misMatchedRowList to set
	 */
	public void setMisMatchedRowList(List<MisMatchedRow> misMatchedRowList) {
		this.misMatchedRowList = misMatchedRowList;
	}

}
