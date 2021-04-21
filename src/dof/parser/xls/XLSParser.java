package dof.parser.xls;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.Date;
import java.util.Iterator;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

import dof.parser.Parser;
import dof.util.SQL;

public abstract class XLSParser extends Parser {

	protected HSSFRow currentRow;
	protected boolean ignoreHiddenSheet = false;

	public XLSParser(Connection connection, String fileName, String[] args) {
		super(connection, fileName, args);
	}

	protected abstract void processRow(HSSFRow row, int rowIndex, HSSFSheet sheet)
			throws IOException;

	@Override
	public void process(boolean renameToDone) throws IOException {

		System.out.println("Iniciando processamento em " + getFileNameToShow());
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(fileName);

			HSSFWorkbook workbook = new HSSFWorkbook(fis);
			int sheetIndex = 0;
			do {
				HSSFSheet sheet;

				sheet = getSheetAt(workbook, sheetIndex);

				if (sheet == null)
					break;

				currentRow = null;

				if (beforeProcessSheet(sheet, sheetIndex)) {
					System.out.println("Processando planilha " + sheet.getSheetName() + "...");
					boolean hidden = workbook.isSheetHidden(sheetIndex);

					processSheet(sheet, sheetIndex, hidden);
				} else {
					System.out.println("Ignorando planilha " + sheet.getSheetName());
				}

				sheetIndex++;
			} while (true);

			finished = true;
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			if (fis != null)
				fis.close();

			if (finished && renameToDone)
				renameToDone();

		}

	}

	private HSSFSheet getSheetAt(HSSFWorkbook workbook, int sheetIndex) {
		HSSFSheet sheet;
		try {
			sheet = workbook.getSheetAt(sheetIndex);
		} catch (Exception e) {
			sheet = null;
		}
		return sheet;
	}

	/**
	 * Retornar true garante que a planilha será processada. O método também
	 * deve ser sobrescrito nas classes descendentes para realizar algum
	 * pré-processamento antes de iniciar a importação da Sheet.
	 */
	protected boolean beforeProcessSheet(HSSFSheet sheet, int sheetIndex) {
		return true;
	}

	private void processSheet(HSSFSheet sheet, int sheetIndex, boolean hidden) throws IOException {

		if (hidden && ignoreHiddenSheet) {
			System.out.println("AVISO: planilha " + sheet.getSheetName() + " oculta foi ignorada");
			return;
		}

		// if (hidden)
		// throw new RuntimeException("AVISO: planilha "
		// + sheet.getSheetName() + " oculta");

		String sheetName = sheet.getSheetName();

		int rowIndex = 1; // começa da linha 1, igual ao EXCEL

		System.out.println("Iniciando processamento...");

		Iterator<Row> rows = sheet.rowIterator();
		while (rows.hasNext()) {
			HSSFRow row = (HSSFRow) rows.next();
			System.out.println(sheetName + " - Processando linha " + rowIndex);
			processRow(row, rowIndex, sheet);

			rowIndex++;
		}
	}

	protected String sqlValue(String colName, HSSFCell cell) {
		Class<?> clazz = dataType(colName);

		String v = sqlValue(clazz, cell);
		return v;
	}

	protected String sqlValue(Class<?> clazz, HSSFCell cell) {
		// String s = cell.getStringCellValue();
		// if (s == null)
		// return "null";
		// if (s.equals(""))
		// return "null";

		if (clazz.equals(Integer.class)) {
			Double v = cell.getNumericCellValue();
			Integer i = (int) Math.round(v);
			return SQL.fieldValue(i);
		}

		if (clazz.equals(Long.class)) {
			Double v = cell.getNumericCellValue();
			Long i = Math.round(v);
			return SQL.fieldValue(i);
		}

		if (clazz.equals(Double.class)) {
			Double v = cell.getNumericCellValue();
			return SQL.fieldValue(v);
		}

		if (clazz.equals(Date.class)) {
			Date v = cell.getDateCellValue();
			return SQL.fieldValue(v);
		}

		if (clazz.equals(String.class)) {
			String v = cell.getStringCellValue();
			return SQL.fieldValue(v);
		}

		throw new RuntimeException("Tipo não suportado: " + clazz);
	}

	protected Class<?> dataType(String colName) {
		return String.class;
	}

	protected boolean nullValue(Cell cell) {
		if (cell.getCellType() == Cell.CELL_TYPE_BLANK)
			return true;
		if (cell.getCellType() == Cell.CELL_TYPE_STRING) {
			String s = cell.getStringCellValue();
			if (s == null)
				return true;
			if (s.equals(""))
				return true;
		}
		return false;
	}

}
