package com.java.main.aggregation;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.StructField;

import com.java.main.beans.MisMatchedRow;
import com.java.main.beans.MisMatchedRows;
import com.java.main.context.GetJavaSparkContext;
import com.java.main.utils.MatchedRecords;
import com.java.main.utils.RDDUtils;

public class AggregationStatisticsDF implements Serializable{


	/**
	 * 
	 */
	private static final long serialVersionUID = -8714138426802237460L;

	public DataFrame getAggregationStatistics(DataFrame df) {

		return df.describe();

	}

	public void getAggregationComparison(DataFrame summary1,
			DataFrame summary2) {
		ArrayList<String> keyColumnName = new ArrayList<String>();
		ArrayList<String> colNames = new ArrayList<String>();
		StructField[] fields = summary1.schema().fields();
		for (StructField field : fields) {
			colNames.add(field.name());
		}
		keyColumnName.add("summary");
		summary1.join(summary2, "summary").show();
		/*MatchedRecords matchedRecords = new MatchedRecords(summary1, summary1,
				keyColumnName, GetJavaSparkContext.getSqlContext());
		DataFrame mergedSummary = matchedRecords.fetMatchingRecords(true);
		mergedSummary.show();
		MisMatchedRows statSummary = RDDUtils.misMatchedRows(
				mergedSummary.toJavaRDD(), colNames);
		
		for( MisMatchedRow misMatchedRow: statSummary.getMisMatchedRowList()){
			System.out.println(misMatchedRow.getRow() + "\t" + misMatchedRow.getMisMatchedCols());
		}
*/
	}
		
		
	

}
