package com.java.main.beans;

import java.io.Serializable;

public class MisMatchedValues implements Serializable {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String colName;
	private String srcVal;
	private String destVal;
	/**
	 * @return the srcVal
	 */
	public String getSrcVal() {
		return srcVal;
	}
	/**
	 * @param srcVal the srcVal to set
	 */
	public void setSrcVal(String srcVal) {
		this.srcVal = srcVal;
	}
	/**
	 * @return the destVal
	 */
	public String getDestVal() {
		return destVal;
	}
	/**
	 * @param destVal the destVal to set
	 */
	public void setDestVal(String destVal) {
		this.destVal = destVal;
	}
	/**
	 * @return the colName
	 */
	public String getColName() {
		return colName;
	}
	/**
	 * @param colName the colName to set
	 */
	public void setColName(String colName) {
		this.colName = colName;
	}

}
