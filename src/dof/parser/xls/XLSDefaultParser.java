package dof.parser.xls;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.ss.usermodel.Cell;

public abstract class XLSDefaultParser extends XLSParser {

	protected boolean firstRowHasHeader = true;

	protected List<String> columnNames = new ArrayList<String>();

	public XLSDefaultParser(Connection connection, String fileName, String[] args) {
		super(connection, fileName, args);
	}

	protected abstract void processDataRow(HSSFRow row, int rowIndex);

	@Override
	protected void processRow(HSSFRow row, int rowIndex, HSSFSheet sheet) {
		currentRow = row;
		if ((firstRowHasHeader) && (rowIndex == 1))
			processHeader(row, sheet);
		else
			processDataRow(row, rowIndex);

	}

	protected void processHeader(HSSFRow row, HSSFSheet sheet) {
		Iterator<Cell> cells = row.cellIterator();

		List<Cell> data = new ArrayList<Cell>();
		while (cells.hasNext()) {
			HSSFCell cell = (HSSFCell) cells.next();
			String s = cell.getStringCellValue();
			data.add(cell);
			foundColumnName(s);
			columnNames.add(s);
		}
		System.out.println();

	}

	protected void foundColumnName(String s) {
		// classes descendentes
	}



}
