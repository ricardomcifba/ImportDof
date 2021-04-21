package dof.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class SQL {


	public static boolean LOG_SQL = false;

	public static String fieldValue(Boolean b) {
		if (b == null)
			return "null";
		if (b)
			return "true";
		else
			return "false";
	}

	public static String fieldValue(String s) {
		if (s == null)
			return "null";

		s = s.trim();

		if (s.equals(""))
			return "null";

		s = s.replace("'", "''");
		s = s.replace("\\", "\\\\");
		return "'" + s + "'";
	}

	public static String fieldValue(Integer i) {
		if (i == null)
			return "null";
		return i + "";
	}

	
	public static String fieldValue(Long l) {
		if (l == null)
			return "null";
		return l + "";
	}

	public static String fieldValue(Double d) {
		if (d == null)
			return "null";
		return d + "";
	}

	public static String fieldValue(Date d) {
		if (d == null)
			return "null";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		return "'" + sdf.format(d) + "'";
	}

	public static String timestampFieldValue(Date d) {
		if (d == null)
			return "null";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		return "'" + sdf.format(d) + "'";
	}

	public static boolean isIOError(SQLException e) {
		String msg = e.getMessage();
		msg = msg.toUpperCase();
		if (msg.contains("E/S"))
			return true;
		if (msg.contains("I/O"))
			return true;
		return false;
	}

	public static ResultSet query(Connection connection, String sql) throws SQLException, IOException {
		try {
			if (LOG_SQL)
				System.out.println(sql);
			Statement s = connection.createStatement();
//			System.out.println(sql);
			return s.executeQuery(sql);
		} catch (SQLException e) {
			if (SQL.isIOError(e))
				throw new IOException(e);
			else
				throw e;
		}
		
	}
	
	public static ArrayList<String> queryStringList(Connection connection, String sql, String columnName) throws SQLException, IOException {
		ArrayList<String> r = new ArrayList<String>();
		ResultSet rs = query(connection, sql);
		while (rs.next()) {
			r.add(rs.getString(columnName));
		}
		return r;
	}

	public static void execSQL(Connection connection, String sql) throws IOException {
		try {
			Statement s = connection.createStatement();
			if (LOG_SQL)
				System.out.println(sql);
			s.execute(sql);
		} catch (SQLException e) {
			System.out.println("Erro no comando: " + sql);
			if (isIOError(e))
				throw new IOException(e);
			throw new RuntimeException(e);
		}
	}

	public static String compositeFilter(String... filters) {
		String r = "";
		for (String f : filters) {
			if (!r.equals(""))
				r += " and ";
			r += f;
		}
		return r;
	}
}
