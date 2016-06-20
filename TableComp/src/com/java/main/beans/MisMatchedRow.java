package com.java.main.beans;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.sql.Row;

public class MisMatchedRow implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private ArrayList<String> row;
	
	private ArrayList<String> misMatchedCols = new ArrayList<String>();
	private ArrayList<MisMatchedValues> srcDestMisValues = new ArrayList<MisMatchedValues>();

	/**
	 * @return the misMatchedCols
	 */
	public ArrayList<String> getMisMatchedCols() {
		return misMatchedCols;
	}

	/**
	 * @param misMatchedCols the misMatchedCols to set
	 */
	public void setMisMatchedCols(ArrayList<String> misMatchedCols) {
		this.misMatchedCols = misMatchedCols;
	}

	

	/**
	 * @return the row
	 */
	public ArrayList<String> getRow() {
		return row;
	}

	/**
	 * @param row the row to set
	 */
	public void setRow(ArrayList<String> row) {
		this.row = row;
	}

	/**
	 * @return the srcDestMisValues
	 */
	public ArrayList<MisMatchedValues> getSrcDestMisValues() {
		return srcDestMisValues;
	}

	/**
	 * @param srcDestMisValues the srcDestMisValues to set
	 */
	public void setSrcDestMisValues(ArrayList<MisMatchedValues> srcDestMisValues) {
		this.srcDestMisValues = srcDestMisValues;
	}



}
