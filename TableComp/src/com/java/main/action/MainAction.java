package com.java.main.action;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import com.java.main.beans.FinalSummaryBean;
import com.java.main.processor.Processor;
import com.java.main.ui.ConfigurationDetailsBean;
import com.java.main.ui.ConnectionDetailsBean;

public class MainAction {

	public HashMap<String, String> getColumns(
			ConnectionDetailsBean connectionDetail)
			throws FileNotFoundException {
		HashMap<String, String> colNamesTypes = new LinkedHashMap<String, String>();
		String srcFile = connectionDetail.getSrcFileAbsolutePath();
		Processor processor = new Processor();
		if (connectionDetail.isCsvSource()) {
			colNamesTypes = processor.getColumnsDetails(srcFile);
		}else{
			colNamesTypes = processor.getParqueteColumnsDetails(srcFile);
		}

		return colNamesTypes;

	}

	public void getComparisonResults(ConfigurationDetailsBean cdb) throws NoSuchAlgorithmException, IOException {
		
			Processor.driver(cdb);
		
	}

}
