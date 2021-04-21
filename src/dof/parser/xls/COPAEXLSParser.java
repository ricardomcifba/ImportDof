package dof.parser.xls;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.ss.usermodel.Cell;

import dof.util.SQL;

public class COPAEXLSParser extends XLSDefaultParser {

	private String fieldDeclarationsForInsert;
	private String fieldValuesForInsert;
	private String fieldAssignmentsForUpdate;

	private int localidade;
	private int setor;
	private int saa;
	private int ano;
	private int mes;

	private Boolean setorial = null;

	public COPAEXLSParser(Connection connection, String fileName, String[] args) {
		super(connection, fileName, args);
	}

	@Override
	protected void processDataRow(HSSFRow row, int rowIndex) {
		Iterator<Cell> cells = row.cellIterator();
		int colIndex = 0;

		resetSQLs();

		while (cells.hasNext()) {
			String colName = columnNames.get(colIndex);
			HSSFCell cell = (HSSFCell) cells.next();

			importData(colName, cell);
			colIndex++;
			// System.out.println("Check colIndex=" + colIndex);
		}
		try {
			processSQL();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private void processSQL() throws SQLException {
		if (setorial == null)
			throw new RuntimeException(
					"Não foi possível distinguir se esta planilha é setorial ou por SAA");
		if (setorial)
			processSQLSetorial();
		else
			processSQLSAA();
	}

	private void processSQLSAA() throws SQLException {
		Statement s = connection.createStatement();

		String sql = updateSQLSAA();

		s.execute(sql);
		int u = s.getUpdateCount();
		if (u > 0)
			return;

		sql = insertSQLSAA();
		s.execute(sql);
	}

	private void processSQLSetorial() throws SQLException {
		Statement s = connection.createStatement();

		String sql = updateSQLSetorial();

		s.execute(sql);
		int u = s.getUpdateCount();
		if (u > 0)
			return;

		sql = insertSQLSetorial();
		s.execute(sql);
	}

	private String insertSQLSetorial() {
		fieldDeclarationsForInsert = "localidade, setor, ano, mes, " + fieldDeclarationsForInsert
				+ ", updated";

		String updated = SQL.fieldValue(fileLastModifiedDate());
		fieldValuesForInsert = SQL.fieldValue(localidade) + ", " + SQL.fieldValue(setor) + ", "
				+ SQL.fieldValue(ano) + ", " + SQL.fieldValue(mes) + ", " + fieldValuesForInsert + ", "
				+ updated;

		String sql = "insert into doo_volumesetorial (" + fieldDeclarationsForInsert + ") values ("
				+ fieldValuesForInsert + ");";
		return sql;
	}

	private String updateSQLSetorial() {

		String updated = SQL.fieldValue(this.fileLastModifiedDate());
		String sql = "update doo_volumesetorial set " + fieldAssignmentsForUpdate + ", updated = "
				+ updated + " where localidade = " + SQL.fieldValue(localidade) + " and setor = "
				+ SQL.fieldValue(setor) + " and ano = " + SQL.fieldValue(ano) + " and mes = "
				+ SQL.fieldValue(mes) + ";";
		return sql;
	}

	private String insertSQLSAA() {
		fieldDeclarationsForInsert = "ano, mes, " + fieldDeclarationsForInsert + ", updated";

		String updated = SQL.fieldValue(fileLastModifiedDate());
		fieldValuesForInsert = SQL.fieldValue(ano) + ", " + SQL.fieldValue(mes) + ", "
				+ fieldValuesForInsert + ", " + updated;

		String sql = "insert into doo_volumesaa (" + fieldDeclarationsForInsert + ") values ("
				+ fieldValuesForInsert + ");";
		return sql;
	}

	private String updateSQLSAA() {

		String updated = SQL.fieldValue(this.fileLastModifiedDate());
		String sql = "update doo_volumesaa set " + fieldAssignmentsForUpdate + ", updated = "
				+ updated + " where saa = " + SQL.fieldValue(saa) + " and ano = " + SQL.fieldValue(ano)
				+ " and mes = " + SQL.fieldValue(mes) + ";";
		return sql;
	}

	private void resetSQLs() {
		fieldDeclarationsForInsert = "";
		fieldValuesForInsert = "";
		fieldAssignmentsForUpdate = "";
	}

	private void importData(String colName, HSSFCell cell) {
		parseKeyFields(colName, cell);
		String sv = sqlValue(colName, cell);
		fieldDeclarationsForInsert = appendByComma(fieldDeclarationsForInsert, colName);
		fieldValuesForInsert = appendByComma(fieldValuesForInsert, sv);
		String sentence = colName + " = " + sv;
		fieldAssignmentsForUpdate = appendByComma(fieldAssignmentsForUpdate, sentence);
	}

	private void parseKeyFields(String colName, HSSFCell cell) {
		colName = colName.toUpperCase();
		if (colName.equals("COD_SETOR")) {
			String s = cell.getStringCellValue();
			parseLocalSetor(s);
			return;
		}
		if (colName.equals("SAA")) {
			saa = (int) cell.getNumericCellValue();
			return;
		}
		if (colName.equals("DATA")) {
			Date d = cell.getDateCellValue();
			parseMesAno(d);
			return;
		}
	}

	private void parseLocalSetor(String s) {
		// Formato padrão: "S41/900-1"
		if (!s.startsWith("S"))
			throw new RuntimeException("COD_SETOR não reconhecido (deveria iniciar pela letra 'S')");
		s = s.substring(1);
		int setfim = s.indexOf('/');

		String ssetor = s.substring(0, setfim);
		setor = Integer.parseInt(ssetor);

		if (!s.endsWith("-1"))
			throw new RuntimeException(
					"COD_SETOR não reconhecido (deveria terminar pela expressão '-1')");
		s = s.replace("-1", "");
		String slocal = s.substring(setfim + 1);
		localidade = Integer.parseInt(slocal);
	}

//	@Deprecated
//	private void parseLocalSetor_Old(String s) {
//		if (!s.startsWith("S"))
//			throw new RuntimeException("COD_SETOR não reconhecido (falta a letra 'S')");
//		String set = s.substring(1, 3);
//		setor = Integer.parseInt(set);
//
//		// System.out.println("Setor: " + set);
//		String t = s.substring(3, 4);
//		if (!t.equals("/"))
//			throw new RuntimeException("COD_SETOR não reconhecido (falta a '/')");
//		int iniLocal = 4;
//		s = s.substring(iniLocal);
//		if (!s.endsWith("-1"))
//			throw new RuntimeException("COD_SETOR não reconhecido (não termina com '-1')");
//		s = s.replace("-1", "");
//
//		System.out.println("Localidade: " + s);
//		localidade = Integer.parseInt(s);
//	}

	private void parseMesAno(Date d) {
		GregorianCalendar c = new GregorianCalendar();
		c.setTime(d);
		ano = c.get(Calendar.YEAR);
		mes = c.get(Calendar.MONTH) + 1;

		// ano = d.getYear();
		// mes = d.getMonth();
	}

	private String appendByComma(String s, String v) {
		if (s == null)
			s = "";
		if (!s.equals(""))
			s = s + ", ";

		s = s + v;
		return s;
	}

	protected boolean beforeProcessSheet(HSSFSheet sheet, int sheetIndex) {
		if (sheet.getSheetName().equalsIgnoreCase("Dados"))
			return true;
		else
			return false;
	}

	@Override
	protected void foundColumnName(String s) {
		if (s.equalsIgnoreCase("COD_SETOR"))
			setorial = true;
		if (s.equalsIgnoreCase("SAA_1")) {
			if (setorial != null) {
				if (setorial)
					throw new RuntimeException(
							"Não foi possível determinar se os dados são setoriais ou por SAA");
			}

			setorial = false;
		}
		super.foundColumnName(s);
	}

	@Override
	protected Class<?> dataType(String colName) {
		colName = colName.toUpperCase();
		
		if (setorial) {
			
			if (colName.equals("COD_SAA"))
				return Integer.class;
			if (colName.equals("COD_UN"))
				return String.class;
			if (colName.equals("COD_SETOR"))
				return String.class;

		} else {
		
			if (colName.equals("SAA"))
				return Integer.class;
			if (colName.equals("SAA_1"))
				return String.class;
			if (colName.equals("UNID"))
				return String.class;
			if (colName.equals("SUPER"))
				return String.class;

		}
		
		if (colName.equals("DATA"))
			return Date.class;
		if (colName.equals("EXT_REDE"))
			return Double.class;
		if (colName.equals("EXT_ADUTORA"))
			return Double.class;
		return Integer.class;
	}

}
