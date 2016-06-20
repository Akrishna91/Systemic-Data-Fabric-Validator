package com.java.main.beans;

import java.io.Serializable;

public class StatisticSummary implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5420365115190640476L;
	private String statFunc;
	private Integer[] values;
	public String getStatFunc() {
		return statFunc;
	}
	public void setStatFunc(String statFunc) {
		this.statFunc = statFunc;
	}
	public Integer[] getValues() {
		return values;
	}
	public void setValues(Integer[] values) {
		this.values = values;
	}

}
