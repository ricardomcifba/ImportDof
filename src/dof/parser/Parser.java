package dof.parser;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import dof.util.SQL;

public abstract class Parser {

	public final boolean SHOW_SPENT_TIMES = false;

	// public static boolean LOG_SQL = false;

	public static boolean FORCE_UPDATE = false;

	protected String fileName;

	protected boolean finished = false;

	protected boolean fileIsValid = true;

	protected Connection connection;

	protected static HashMap<String, Integer> idMetricas = new HashMap<String, Integer>();

	/**
	 * Campos usados para gerenciar o tempo de processamento de cada etapa, para
	 * estudo e melhoria de performance de importação
	 */
	protected Map<String, Long> spentTimeMonitor = new HashMap<String, Long>();

	protected Map<String, Long> monitoredActionsStartTime = new HashMap<String, Long>();

	private boolean fileWasRenamedToDone = false;

	private Date fileLastModifiedDate;

	public Parser(Connection connection, String fileName, String[] args) {
		this.connection = connection;
		this.fileName = fileName;
		parseArgs(args);
	}

	protected void parseArgs(String[] args) {
		// sobrescrever em classes descendentes
	}

	protected void undoRenameToDone() throws IOException {
		String name = fileName;
		File f = new File(name);
		String done = name + "_done";
		File fd = new File(done);
		if (!fd.exists())
			throw new RuntimeException("Arquivo inexistente: " + done);
		if (f.exists()) {
			f.delete();
			// f = new File(name);
		}

		if (!fd.renameTo(f))
			throw new RuntimeException("Não foi possível renomear de " + done + " para " + name);
	}

	protected void renameToDone() {
		String fullName = fileName;

		if (endsWithDone(fullName)) {
			fileWasRenamedToDone = true;
			return;
		}

		File f = new File(fullName);
		if (!f.exists())
			throw new RuntimeException("Arquivo inexistente: " + fullName);

		if (!fullName.contains("."))
			fullName = fullName + ".txt";
		String done = fullName + "_done";
		File fd = new File(done);
		int i = 1;
		do {
			done = fullName + "_done";
			if (i != 1)
				done = done + i;
			fd = new File(done);
			if (fd.exists())
				i++;
		} while (fd.exists());

		if (!f.renameTo(fd)) {
			System.out.println("AVISO: Não foi possível renomear de " + fullName + " para " + done);
			fileWasRenamedToDone = false;
		} else
			fileWasRenamedToDone = true;
	}

	protected void afterFinished() {
		// sobrescrever em filhas
	}

	protected boolean shouldProcessThisFile() {
		return true;
	}

	protected String getFileNameToShow() {
		return getSimpleFileName();
	}

	@Deprecated
	protected Integer idMetrica(String sigla) throws IOException {
		Integer r = idMetricas.get(sigla);
		if (r != null)
			return r;
		try {
			r = (Integer) singleValueQuery("select id from ind_metrica where sigla = '" + sigla
					+ "'");
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		idMetricas.put(sigla, r);
		return r;
	}

	protected void processRow(int rowId, String content) throws IOException {

	}

	protected static Integer valorInt(String s) {
		if (s == null)
			return null;
		s = s.trim();
		if (s.equals(""))
			return null;
		s = s.replace(".", "");
		return Integer.parseInt(s);
	}

	protected Double valorMonetario(String s) {
		if (s == null)
			return null;
		s = s.trim();
		if (s.equals(""))
			return null;
		long v = Long.parseLong(s);
		return v / 100.0;
	}

	protected Long valorLong(String s) {
		if (s == null)
			return null;
		s = s.trim();
		if (s.equals(""))
			return null;
		s = s.replace(".", "");
		return Long.parseLong(s);
	}

	protected String trim(String content) {
		if (content == null)
			return null;
		return content.trim();
	}

	protected boolean empty(String t) {
		if (t == null)
			return true;

		return t.trim().equals("");
	}

	public Object singleValueQuery(String sql) throws SQLException, IOException {
		ResultSet rs = query(sql);
		if (!rs.next())
			return null;
		Object o = rs.getObject(1);
		return o;
	}

	public void execSQL(String sql) throws IOException {

		SQL.execSQL(connection, sql);
	}

	public ResultSet query(String sql) throws SQLException, IOException {
		return SQL.query(connection, sql);
	}

	protected Double valorDouble(String s) {
		// considera XXX.XXX.XXX,XX
		if (s == null)
			return null;
		s = s.trim();
		if (s.equals(""))
			return null;

		s = s.replace(".", "");
		s = s.replace(",", ".");
		return Double.parseDouble(s);
	}

	// @Override
	// public String toString() {
	// return "Parser / Contexto = " + contexto;
	// }

	protected static String[] splitBetweenConsecSpaces(String text) {
		return splitBetweenConsecSpaces(text, 0);
	}

	protected static String[] splitBetweenConsecSpaces(String text, int start) {
		if (text == null)
			return null;
		text = text.substring(start);

		ArrayList<String> ar = new ArrayList<String>();
		String s = "";
		for (int i = 0; i < text.length(); i++) {
			String c = text.substring(i, i);
			if (c.equals(" ")) {
				if (!s.equals(""))
					ar.add(s);
				s = "";
			} else
				s = s + c;
		}

		String[] ss = new String[ar.size()];
		ss = ar.toArray(ss);
		return ss;

	}

	protected boolean equals(Integer a, Integer b) {
		if (a == null) {
			if (b == null)
				return true;
		} else {
			if (b == null)
				return false;

			if (a.equals(b))
				return true;
		}
		return false;
	}

	protected String getSimpleFileName() {
		File f = new File(fileName);
		return f.getName();
	}

	protected long getSpentTime(String action) {
		Long r = spentTimeMonitor.get(action);
		if (r == null)
			r = (long) 0;
		return r;
	}

	private void addSpentTime(String action, long time) {
		long r = getSpentTime(action);
		r += time;
		spentTimeMonitor.put(action, r);
	}

	public void startMonitoringAction(String action) {
		Long start = monitoredActionsStartTime.get(action);
		if (start != null)
			throw new RuntimeException("Ação já sendo monitorada: " + action);
		Date d = new Date();
		start = d.getTime();
		monitoredActionsStartTime.put(action, start);
	}

	public void endMonitoringAction(String action) {
		Long start = monitoredActionsStartTime.get(action);
		if (start == null)
			throw new RuntimeException("Ação não está sendo monitorada: " + action);
		Date d = new Date();
		long t = d.getTime();
		long elapsed = t - start;
		addSpentTime(action, elapsed);
		monitoredActionsStartTime.remove(action);
	}

	protected void showSpentTimeByAction() {
		Set<String> keySet = spentTimeMonitor.keySet();
		if (keySet.isEmpty())
			return;
		System.out.println("Tempos gastos para:");
		for (String a : keySet) {
			Long t = spentTimeMonitor.get(a);
			System.out.println("  " + a + ": " + t);
		}
	}

	public String getFullPath() {
		return fileName;
	}

	protected boolean emptyQuery(String sql) throws SQLException, IOException {
		ResultSet rs = query(sql);
		if (rs.next())
			return false;
		return true;
	}

	public boolean fileWasRenamedToDone() {
		return fileWasRenamedToDone;
	}

	public abstract void process(boolean renameToDone) throws IOException;

	public static boolean endsWithDone(String fileName) {
		if (fileName.toLowerCase().endsWith("done"))
			return true;
		int length = fileName.length();
		String s = fileName.substring(0, length - 1);
		if (s.isEmpty())
			return false;

		if (endsWithDone(s))
			return true;
		return false;
	}

	protected int ifNull(Integer valor, Integer valorIfNull) {
		if (valor == null)
			return valorIfNull;
		else
			return valor;
	}

	protected double ifNull(Double valor, Double valorIfNull) {
		if (valor == null)
			return valorIfNull;
		else
			return valor;
	}

	public Date fileLastModifiedDate() {
		if (fileLastModifiedDate == null) {
			File f = new File(fileName);
			fileLastModifiedDate = new Date(f.lastModified());
		}
		return fileLastModifiedDate;
	}

}
