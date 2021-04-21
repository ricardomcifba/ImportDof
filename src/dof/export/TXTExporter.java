package dof.export;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class TXTExporter {

	private static FileWriter fileWriter;

	private String separator = ";";

	private ResultSetMetaData metaData;

	public TXTExporter(String filePath) throws IOException {
		super();
		File outputFile = new File(filePath);
		fileWriter = new FileWriter(outputFile, true);

	}

	public void exportData(ResultSet resultSet) throws SQLException, IOException {
		metaData = resultSet.getMetaData();
		while (resultSet.next()) {
			exportCurrentRow(resultSet);
		}
	}

	public void close() throws IOException {
		fileWriter.close();
	}

	private void exportCurrentRow(ResultSet resultSet) throws SQLException, IOException {
		int columnCount = metaData.getColumnCount();
		if (columnCount == 0)
			throw new RuntimeException("Consulta vazia");

		String r = fieldValue(resultSet, 1);

		for (int i = 2; i <= columnCount; i++) {
			String sv = fieldValue(resultSet, i);
			r = r + separator + sv;
		}
		writeln(r);
	}

	private String fieldValue(ResultSet resultSet, int fieldIndex) throws SQLException {
		int type = metaData.getColumnType(fieldIndex);
		String field = metaData.getColumnName(fieldIndex);
		String sv = fieldValueAsString(resultSet, field, type);
		return sv;
	}

	private String fieldValueAsString(ResultSet resultSet, String fieldName, int type) throws SQLException {
		String s = resultSet.getString(fieldName);
		if (s == null)
			return "";
		if (s.equals("null"))
			return "";
		return s;
//		if (type == 12)
//			return resultSet.getString(fieldName);
//		if (type == -5) {
//			Long v = resultSet.getLong(fieldName);
//			return v + "";
//		}
//		throw new RuntimeException(fieldName + " - Tipo de dados não suportado: " + type);
		// TODO Auto-generated method stub
	}

	public void exportHeader(ResultSet rs) throws SQLException, IOException {
		metaData = rs.getMetaData();
		int columnCount = metaData.getColumnCount();
		if (columnCount == 0)
			throw new RuntimeException("Consulta vazia");

		String r = fieldName(1);

		for (int i = 2; i <= columnCount; i++) {
			String sv = fieldName(i);
			r = r + separator + sv;
		}
		writeln(r);
	}

	private String fieldName(int fieldIndex) throws SQLException {
		return metaData.getColumnName(fieldIndex);
	}

	protected static void writeln() throws IOException {
		writeln("");
	}

	protected static void writeln(String s) throws IOException {
		fileWriter.write(s + "\r\n");
	}

}
