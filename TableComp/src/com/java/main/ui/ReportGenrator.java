package com.java.main.ui;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.util.CellRangeAddress;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Color;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.java.main.beans.AggregationMismatches;
import com.java.main.beans.FinalSummaryBean;
import com.java.main.beans.MisMatchedRow;
import com.java.main.beans.MisMatchedValues;
import com.java.main.constants.AggregationFuncNames;
import com.java.main.constants.RulesMatchingStatus;

public class ReportGenrator {
	public static void genrateReport(ConfigurationDetailsBean ConfigBean,
			FinalSummaryBean finalSummary, String path) throws Exception {

		// Create blank workbook
		XSSFWorkbook workbook = new XSSFWorkbook();
		// Create a blank sheet
		XSSFSheet spreadsheet = workbook.createSheet("Column Report");
		// Create row object
		XSSFRow row;

		CellStyle headerStyle = workbook.createCellStyle();
		headerStyle.setFillBackgroundColor(IndexedColors.LIGHT_GREEN.getIndex());
		headerStyle.setFillPattern(CellStyle.ALIGN_CENTER);
		headerStyle.setBorderBottom(HSSFCellStyle.BORDER_THIN);
		headerStyle.setBorderTop(HSSFCellStyle.BORDER_THIN);
		headerStyle.setBorderRight(HSSFCellStyle.BORDER_THIN);
		headerStyle.setBorderLeft(HSSFCellStyle.BORDER_THIN);
		// Creating Header
		int colNo = 0;
		Cell headerCell;

		XSSFRow colRow;
		Cell colCell;
		int cellNo = 1;
		for (int i = 0; i < ConfigBean.getColumnNames().size(); i++) {
			colRow = spreadsheet.createRow(cellNo);
			colCell = colRow.createCell(0);
			colCell.setCellValue(ConfigBean.getColumnNames().get(i));
			spreadsheet.addMergedRegion(new CellRangeAddress(cellNo,
					cellNo + 1, 0, 0));
			colRow = spreadsheet.createRow(cellNo + 1);
			cellNo = cellNo + 2;
		}

		row = spreadsheet.createRow(0);

		headerCell = row.createCell(colNo++);
		headerCell.setCellValue("Column Name");

		if (ConfigBean.isUniquenessRule()) {
			headerCell = row.createCell(colNo++);
			headerCell.setCellValue("Uniqueness");
			spreadsheet.addMergedRegion(new CellRangeAddress(0, 0, colNo - 1,
					colNo++));

			XSSFRow resultRow;
			Cell resultCell;

			int rowNum = 1;
			int colNum = colNo - 2;

			for (int i = 0; i < ConfigBean.getColumnNames().size(); i++) {
				String colName = ConfigBean.getColumnNames().get(i);

				resultRow = spreadsheet.getRow(rowNum);

				if ((finalSummary.getDistinctRuleResults().get(colName) != null)
						&& !finalSummary.getDistinctRuleResults().get(colName)
								.getStatus()
								.equals(RulesMatchingStatus.MATCHED)) {
					resultCell = resultRow.createCell(colNum);
					resultCell.setCellValue("Src - "
							+ finalSummary.getDistinctRuleResults()
									.get(colName).getSourceValues());

					resultCell = resultRow.createCell(colNum + 1);
					resultCell.setCellValue("Dest - "
							+ finalSummary.getDistinctRuleResults()
									.get(colName).getDestValues());
				} else {
					spreadsheet.addMergedRegion(new CellRangeAddress(rowNum,
							rowNum, colNum, colNum + 1));
				}

				resultRow = spreadsheet.getRow(rowNum + 1);
				resultCell = resultRow.createCell(colNum);
				try {
					resultCell.setCellValue("Result - "
							+ finalSummary.getDistinctRuleResults()
									.get(colName).getStatus());
				} catch (NullPointerException e) {
					resultCell.setCellValue("Result - " + "No Rule Genrated");
				}

				spreadsheet.addMergedRegion(new CellRangeAddress(rowNum + 1,
						rowNum + 1, colNum, colNum + 1));

				rowNum = rowNum + 2;
			}
		}

		if (ConfigBean.isPossibleValueRule()) {
			headerCell = row.createCell(colNo++);
			headerCell.setCellValue("Possible Values");
			spreadsheet.addMergedRegion(new CellRangeAddress(0, 0, colNo - 1,
					colNo++));

			XSSFRow resultRow;
			Cell resultCell;

			int rowNum = 1;
			int colNum = colNo - 2;

			for (int i = 0; i < ConfigBean.getColumnNames().size(); i++) {
				String colName = ConfigBean.getColumnNames().get(i);
				resultRow = spreadsheet.getRow(rowNum);

				if ((finalSummary.getPossibleValueRuleResults().get(colName) != null)
						&& !finalSummary.getPossibleValueRuleResults()
								.get(colName).getStatus()
								.equals(RulesMatchingStatus.MATCHED)) {
					resultCell = resultRow.createCell(colNum);
					resultCell.setCellValue("Src - "
							+ finalSummary.getPossibleValueRuleResults()
									.get(colName).getSourceValues());

					resultCell = resultRow.createCell(colNum + 1);
					resultCell.setCellValue("Dest - "
							+ finalSummary.getPossibleValueRuleResults()
									.get(colName).getDestValues());
				}

				resultRow = spreadsheet.getRow(rowNum + 1);
				resultCell = resultRow.createCell(colNum);
				try {
					resultCell.setCellValue("Result - "
							+ finalSummary.getPossibleValueRuleResults()
									.get(colName).getStatus());

				} catch (NullPointerException e) {
					resultCell.setCellValue("Result - " + "No Rule Genrated");
				}

				spreadsheet.addMergedRegion(new CellRangeAddress(rowNum + 1,
						rowNum + 1, colNum, colNum + 1));

				rowNum = rowNum + 2;
			}
		}

		if (ConfigBean.isDateTypeRule()) {
			headerCell = row.createCell(colNo++);
			headerCell.setCellValue("Date Type");
			spreadsheet.addMergedRegion(new CellRangeAddress(0, 0, colNo - 1,
					colNo++));

			XSSFRow resultRow;
			Cell resultCell;

			int rowNum = 1;
			int colNum = colNo - 2;

			for (int i = 0; i < ConfigBean.getColumnNames().size(); i++) {
				String colName = ConfigBean.getColumnNames().get(i);
				resultRow = spreadsheet.getRow(rowNum);
				resultCell = resultRow.createCell(colNum);
				resultCell.setCellValue("Src - " + "100");

				resultCell = resultRow.createCell(colNum + 1);
				resultCell.setCellValue("Dest - " + "100");

				resultRow = spreadsheet.getRow(rowNum + 1);
				resultCell = resultRow.createCell(colNum);
				resultCell.setCellValue("Result - " + "Matched");

				spreadsheet.addMergedRegion(new CellRangeAddress(rowNum + 1,
						rowNum + 1, colNum, colNum + 1));

				rowNum = rowNum + 2;
			}
		}
		if (ConfigBean.isSummationRule()) {
			Sheet sheet = workbook.createSheet("Aggregation Summary");
			
			DataFrame srcAggSummary = finalSummary.getSrcAggSummary();
			DataFrame destAggSummary = finalSummary.getDestAggSummary();
			int rowNum = 0;
			org.apache.poi.ss.usermodel.Row row1 = sheet.createRow(rowNum);
			rowNum++;
			int cellNum = 0;
			for(String col : srcAggSummary.columns()){
				row1.createCell(cellNum).setCellValue(col);
				cellNum++;
			}
			for(Row r : srcAggSummary.collectAsList()){
				row1 = sheet.createRow(rowNum);
				cellNum = 0;
				for(int i = 0; i < r.size(); i++){
					row1.createCell(cellNum).setCellValue(r.get(i).toString() );
					cellNum++;
				}
				rowNum++;
			}
			rowNum += 2;
			cellNum = 0;
			row1 = sheet.createRow(rowNum);
			rowNum++;
			row1.createCell(cellNum).setCellValue("Destination Summary");
			for(Row r : destAggSummary.collectAsList()){
				row1 = sheet.createRow(rowNum);
				cellNum = 0;
				for(int i = 0; i < r.size(); i++){
					row1.createCell(cellNum).setCellValue(r.get(i).toString() );
					cellNum++;
				}
				rowNum++;
			}
			
		}
		List<MisMatchedRow> misMatchedRows = finalSummary.getMisMatchedRows();
		if (misMatchedRows != null && misMatchedRows.size() > 0) {

			Sheet sheet = workbook.createSheet("Mis Matched Row Summary");
			int rowNum = 0;
			org.apache.poi.ss.usermodel.Row row1 = sheet.createRow(rowNum);
			rowNum++;
			row1.createCell(0).setCellValue("Key Column Names (in order)");
			row1.createCell(1).setCellValue(ConfigBean.getKeyColumnNames().toString());
			sheet.addMergedRegion(new CellRangeAddress(rowNum-1, rowNum-1 , 0, 4));
			row1 = sheet.createRow(rowNum);
			rowNum++;
			int cellNum = 0;
			Cell cell = row1.createCell(cellNum);
			cell.setCellStyle(headerStyle);cell.setCellValue("Sr. No.");
			cell = row1.createCell(++cellNum);
			cell.setCellStyle(headerStyle);cell.setCellValue("Key Column Values");
			cell = row1.createCell(++cellNum);
			cell.setCellStyle(headerStyle);cell.setCellValue("Mis-Matched Column Names");
			cell = row1.createCell(++cellNum);
			cell.setCellStyle(headerStyle);cell.setCellValue("Source Value");
			cell = row1.createCell(++cellNum);
			cell.setCellStyle(headerStyle);cell.setCellValue("Destination Value");
			int misMatchedNum = 0;
			for (MisMatchedRow misMatchedRow : misMatchedRows) {
				cellNum = 0;
				ArrayList<String> currentRow = misMatchedRow.getRow();
				/*//to add mis matched exact row
				row1 = sheet.createRow(rowNum);
				
				row1.createCell(cellNum).setCellValue(currentRow.toString());

				sheet.addMergedRegion(new CellRangeAddress(rowNum, rowNum, 0, 3));
				rowNum++;*/
				ArrayList<String> keyValues = new ArrayList<String>();
				for(Integer keyInd : finalSummary.getKeyColIndex()){
					keyValues.add(currentRow.get(keyInd));
				}
				row1 = sheet.createRow(rowNum);
				row1.createCell(cellNum).setCellValue(++misMatchedNum);
				cellNum++;
				row1.createCell(cellNum).setCellValue(keyValues.toString());
				cellNum++;
				int startRowNum = rowNum;
				for (MisMatchedValues matchedValues : misMatchedRow
						.getSrcDestMisValues()) {
					cellNum = 2;
					row1.createCell(cellNum).setCellValue(matchedValues.getColName());
					cellNum++;
					row1.createCell(cellNum).setCellValue(matchedValues.getSrcVal());
					cellNum++;
					row1.createCell(cellNum).setCellValue(matchedValues.getDestVal());
					rowNum++;
					row1 = sheet.createRow(rowNum);
				}
				sheet.addMergedRegion(new CellRangeAddress(startRowNum, rowNum - 1, 0, 0));
				sheet.addMergedRegion(new CellRangeAddress(startRowNum, rowNum - 1, 1, 1));
			}
		}

		// Creating lost row sheet - Start
		XSSFSheet mismatchedRowSheet = workbook.createSheet("Lost Rows");
		// Create row object
		XSSFRow rows;
		// This data needs to be written (Object[])
		LinkedHashMap<String, String> rowinfo = new LinkedHashMap<String, String>();

		rowinfo.put("Sr. No.", "Lost Row");
		int i = 0;
		for (String row1 : finalSummary.getMissingRows()) {
			rowinfo.put(i + "", row1);
			i++;
		}

		// Iterate over data and write to sheet
		Set<String> newKeyid = rowinfo.keySet();
		int newRowId = 0;

		for (String key : newKeyid) {
			rows = mismatchedRowSheet.createRow(newRowId++);
			Cell cell1 = rows.createCell(0);
			cell1.setCellValue(key);

			Cell cell2 = rows.createCell(1);
			cell2.setCellValue(rowinfo.get(key));
		}

		// Creating mismatched row sheet - End

		// Write the workbook in file system
		FileOutputStream out = new FileOutputStream(new File(path
				+ "/QualityCheckReport.xlsx"));
		workbook.write(out);
		out.close();
		System.out.println("QualityCheckReport.xlsx written successfully");
	}
	
}
