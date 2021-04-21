package dof.parser.xls;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.ss.usermodel.Cell;

public class AvalServXLSParser extends XLSDefaultParser {

	public AvalServXLSParser(Connection connection, String fileName, String[] args) {
		super(connection, fileName, args);
	}

	@Override
	protected void processDataRow(HSSFRow row, int rowIndex) {
		Iterator<Cell> cells = row.cellIterator();
		ArrayList<String> values = new ArrayList<String>();
		int colIndex = 0;

		while (cells.hasNext()) {
			String colName = columnNames.get(colIndex);
			HSSFCell cell = (HSSFCell) cells.next();

			String value = sqlValue(colName, cell);
			values.add(value);
			colIndex++;
			// System.out.println("Check colIndex=" + colIndex);
		}

		try {
			importData(values);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void importData(ArrayList<String> values) throws SQLException, IOException {
		String matricula = getValue(values, "Matrícula");
		if (matricula.equals("null") || matricula.equals("0"))
			return;

		String ts = getValue(values, "Tipo de Serviço");
		if (!ts.equals("null") && !ts.equals("0")) {
			importRetornoServico(values);
		}
		String tag = getValue(values, "Tag");
		if (!tag.equals("null")) {
			if (!tag.trim().equals("")) {
				importTag(values);
			}
		}
	}

	private void importRetornoServico(ArrayList<String> values) throws IOException, SQLException {
		String sql = "select count(*) from amf_retorno "
				+ "where matricula = %m and tiposervico = %t and execucao = %e;";
		sql = assignKeyRetorno(values, sql);

		long i = (Long) singleValueQuery(sql);
		if (i < 1) {

			sql = "insert into amf_retorno (matricula, tiposervico, execucao) "
					+ "values (%m, %t, %e);";
			sql = assignKeyRetorno(values, sql);
			execSQL(sql);
		}
	}

	private String assignKeyRetorno(ArrayList<String> values, String sql) {
		sql = sql.replace("%m", getValue(values, "Matrícula"));
		sql = sql.replace("%t", getValue(values, "Tipo de Serviço"));
		sql = sql.replace("%e", getValue(values, "Data de Execução"));
		return sql;
	}

	private String assignKeyTag(ArrayList<String> values, String sql) {
		sql = sql.replace("%m", getValue(values, "Matrícula"));
		sql = sql.replace("%d", getValue(values, "Data Tag"));
		sql = sql.replace("%t", getValue(values, "Tag"));
		return sql;
	}

	private String getValue(ArrayList<String> values, String column) {
		for (int i = 0; i < columnNames.size(); i++) {
			String n = columnNames.get(i);
			if (mesmaColuna(n, column)) {
				return values.get(i);
			}
		}
		throw new RuntimeException("Coluna não localizada: " + column);
	}

	private boolean mesmaColuna(String a, String b) {
		if (a.equals(b))
			return true;
		return false;
	}

	private void importTag(ArrayList<String> values) throws SQLException, IOException {
		String sql = "select count(*) from amf_tag " + "where matricula = %m and data = %d and tag = %t;";
		sql = assignKeyTag(values, sql);

		long i = (Long) singleValueQuery(sql);
		if (i < 1) {

			sql = "insert into amf_tag (matricula, data, tag) " + "values (%m, %d, %t);";
			sql = assignKeyTag(values, sql);
			execSQL(sql);
		}
	}

	String appendByComma(String s, String v) {
		if (s == null)
			s = "";
		if (!s.equals(""))
			s = s + ", ";

		s = s + v;
		return s;
	}

	// @Override
	// protected void foundColumnName(String s) {
	// if (s.equalsIgnoreCase("COD_SETOR"))
	// setorial = true;
	// if (s.equalsIgnoreCase("SAA_1")) {
	// if (setorial != null) {
	// if (setorial)
	// throw new RuntimeException(
	// "Não foi possível determinar se os dados são setoriais ou por SAA");
	// }
	//
	// setorial = false;
	// }
	// super.foundColumnName(s);
	// }

	@Override
	protected Class<?> dataType(String colName) {
//		colName = colName.toUpperCase();
		if (colName.equals("Matrícula"))
			return Long.class;
		if (colName.equals("Tipo de Serviço"))
			return Integer.class;
		if (colName.equals("Data de Execução"))
			return Date.class;
		if (colName.equals("Tag"))
			return String.class;
		if (colName.equals("Data Tag"))
			return Date.class;
		throw new RuntimeException("Coluna desconhecida: " + colName);
	}

}
