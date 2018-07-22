package com.retail;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ExcelParser {
	private StringBuilder currentString = null;
	private long bytesRead = 0;

	public String parseExcelData(InputStream is) {
		try {
			XSSFWorkbook workbook = new XSSFWorkbook(is);

			// Taking first sheet from the workbook
			XSSFSheet sheet = workbook.getSheetAt(0);

			// Iterate through each rows from first sheet
			Iterator<Row> rowIterator = sheet.iterator();
			currentString = new StringBuilder();
			while (rowIterator.hasNext()) {
				Row row = rowIterator.next();

				// For each row, iterate through each columns
				Iterator<Cell> cellIterator = row.cellIterator();

				while (cellIterator.hasNext()) {

					Cell cell = cellIterator.next();

					switch (cell.getCellType()) {
					case Cell.CELL_TYPE_BOOLEAN:
						bytesRead++;
						currentString.append(cell.getBooleanCellValue() + "\t");
						break;

					case Cell.CELL_TYPE_NUMERIC:
						bytesRead++;
						currentString.append(cell.getNumericCellValue() + "\t");
						break;

					case Cell.CELL_TYPE_STRING:
						bytesRead++;
						currentString.append(cell.getStringCellValue() + "\t");
						break;

					}
				}
				currentString.append("\n");
			}
			is.close();
		} catch (IOException e) {
			System.out.println("IO Exception : File not found " + e);
		}
		return currentString.toString();

	}

	public long getBytesRead() {
		return bytesRead;
	}

}