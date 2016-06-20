package com.java.main.utils;

import java.util.ArrayList;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameHolder;
import org.apache.spark.sql.SQLContext;

public class MatchedRecords {

	// private FileDataFrames fileDFs;
	private ArrayList<String> pkey;
	private SQLContext sqlContext;
	private DataFrame srcDf;
	private DataFrame destDf;
	private DataFrameHolder temp;

	/*
	 * 
	 * public MatchedRecords(FileDataFrames fileDFs, ArrayList<String> pkey,
	 * SQLContext sqlContext) { this.fileDFs = fileDFs;
	 * setSrcDf(fileDFs.getSourceDataFrame());
	 * setDestDf(fileDFs.getDestDataFrame()); this.pkey = pkey; this.sqlContext
	 * = sqlContext; }
	 */

	public MatchedRecords(DataFrame srcDf, DataFrame destDf,
			ArrayList<String> pkey, SQLContext sqlContext) {
		this.srcDf = srcDf;
		this.destDf = destDf;
		this.pkey = pkey;
		this.sqlContext = sqlContext;
	}

	/**
	 * if isMatched true, will fetch matching records in two tables else
	 * unmatched records
	 * 
	 * @param isMatched
	 * @return
	 */
	public DataFrame fetMatchingRecords(Boolean isMatched) {
		DataFrame srcDf = getSrcDf();
		DataFrame destDf = getDestDf();
		srcDf.registerTempTable("SrcTable");
		destDf.registerTempTable("DestTable");
		DataFrame records;
		records = sqlContext.sql(getSQL(isMatched));
		return records;
	}

	private String getSQL(Boolean isMatched) {
		String sql;
		if (isMatched) {
			sql = "SELECT * FROM SrcTable, DestTable ";

		} else {
			sql = "SELECT SrcTable.* FROM SrcTable LEFT JOIN DestTable ";
		}
		sql += getWhereCondition(isMatched);
		System.out.println("SQL-->" + sql);
		return sql;
	}

	private String getWhereCondition(Boolean isMatched) {
		String whereCondition = "";
		String nullCondition = "";
		int length = 1;
		if (pkey.size() > 0) {
			if (isMatched) {
				whereCondition += " WHERE ";
			} else {
				whereCondition += "ON ";
				nullCondition += " WHERE ";
			}
			for (String key : pkey) {
				if (isMatched) {
					whereCondition += ("SrcTable." + key + " = DestTable." + key);
				} else {
					whereCondition += ("SrcTable." + key + " = DestTable." + key);
					nullCondition += " DestTable." + key + " IS NULL ";
				}

				if (!(length == pkey.size())) {
					whereCondition += " AND ";
					nullCondition += " AND ";
					length++;
				}
			}
		}
		if (!isMatched) {
			whereCondition += nullCondition;
		}
		return whereCondition;
	}

	/*
	 * private String getSQL() { String sql =
	 * "SELECT d.* FROM DestTable d INNER JOIN SrcTable s "+getWhereCondition();
	 * //String sql = "SELECT d.* FROM DestTable d where d._id in (1,2,3)";
	 * System.out.println("SQL-->"+sql); return sql; }
	 * 
	 * private String getWhereCondition() { String whereCondition = ""; int
	 * length = 1; if(pkey.size()>0){ whereCondition+="ON "; for(String key :
	 * pkey){ //whereCondition+=("s."+key+" IS NOT NULL AND d."+key+
	 * " IS NOT NULL AND "); whereCondition+=("s."+key+" = d."+key);
	 * if(!(length==pkey.size())){ whereCondition+=" AND "; length++; } } }
	 * return whereCondition; }
	 */

	public ArrayList<String> getPkey() {
		return pkey;
	}

	public void setPkey(ArrayList<String> pkey) {
		this.pkey = pkey;
	}

	public DataFrame getSrcDf() {
		return srcDf;
	}

	public void setSrcDf(DataFrame srcDf) {
		this.srcDf = srcDf;
	}

	public DataFrame getDestDf() {
		return destDf;
	}

	public void setDestDf(DataFrame destDf) {
		this.destDf = destDf;
	}

	/**
	 * @return the temp
	 */
	public DataFrameHolder getTemp() {
		return temp;
	}

	/**
	 * @param temp the temp to set
	 */
	public void setTemp(DataFrameHolder temp) {
		this.temp = temp;
	}

}
