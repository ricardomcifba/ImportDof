package dof;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class SSPendMain {

	private static final int LINHAS_POR_ARQUIVO = 100;

	private static String workingDirOld = "c:/bd/dof/CTSS";

	private static String workingDir = "d:/bd/dof/CTSS";

	private static Connection connection;

	private static Long base = null;

	private static BufferedWriter writer;

	private static String currentDir = getWorkingDir();

	private static int linhasNoArquivo = 0;

	private static int totalGeradas = 0;

	private static int latencia = 10;
	// latência indica o número de dias passados até quando as SS serão
	// atualizadas. Por ex.: se hoje
	// é dia 29, só serão baixadas SS's de até antes do dia 19

	private static boolean gerarBatch = true;
	private static boolean gerarOnline = true;

	private static boolean gerarNovas = true;
	private static boolean gerarPendentes = true;
	private static boolean gerarAntigas = false;

	private static int LIMITE_SS_PENDENTES = 80000;

	private static long LIMIAR_NUMERACAO_BATCH_ONLINE = 800000000;

	public static void main(String[] args) throws IOException, SQLException {

		connection = DriverManager.getConnection(
				"jdbc:postgresql://10.19.152.47:5432/fcfdof", "postgres",
				"fcfp01");

		configuraEscopo(args);

		System.out.println("Gerando arquivos SRC para consulta ao SCI.");
		if (gerarNovas)
			gerarNovas();
		if (gerarPendentes)
			gerarSSPendentes();
		if (gerarAntigas)
			gerarAntigas();
		System.out.println("Concluído.");
	}

	private static void configuraEscopo(String[] args) {
		if (args.length == 0)
			return;

		tratarLatencia(args);

		System.out.println("Parâmetros definidos: " + bySpace(args));
		if (contains(args, "AUTO")) {
			configuraEscopoAuto();
			return;
		}

		if (contains(args, "NOVAS") || contains(args, "PENDENTES")
				|| contains(args, "ANTIGAS")) {
			gerarNovas = false;
			gerarPendentes = false;
			gerarAntigas = false;
		}
		if (contains(args, "NOVAS"))
			gerarNovas = true;
		if (contains(args, "PENDENTES"))
			gerarPendentes = true;
		if (contains(args, "ANTIGAS"))
			gerarAntigas = true;

		if (contains(args, "BATCH") || contains(args, "ONLINE")) {
			gerarBatch = false;
			gerarOnline = false;
		}

		if (contains(args, "BATCH"))
			gerarBatch = true;
		if (contains(args, "ONLINE"))
			gerarOnline = true;

	}

	private static String bySpace(String[] args) {
		String r = "";
		for (String s : args) {
			if (!r.equals(""))
				r += " ";
			r += s;
		}
		return r;
	}

	private static void tratarLatencia(String[] args) {
		String s = extractComplement(args, "LATENCIA=");
		if (s == null)
			return;
		latencia = Integer.parseInt(s);

	}

	private static String extractComplement(String[] args, String prefix) {
		prefix = prefix.toUpperCase();
		for (String s : args) {
			String u = s.toUpperCase();
			if (u.startsWith(prefix))
				return u.replace(prefix, "");
		}
		return null;
	}

	private static void configuraEscopoAuto() {
		int novasBatch = estimarNovasBatch();
		int novasOnline = estimarNovasOnline();
		int b = biggest(novasBatch, novasOnline);
		Integer pendentes = null;
		
		if (b <= 0) { // evita contar pendentes enquanto há alguma nova (batch
						// ou online) sem importar
			// isto porque contar pendentes é despendioso
			pendentes = contarPendentes();
			b = biggest(pendentes, novasBatch, novasOnline);
		}

		if (b <= 0) {
			System.out.println("Não há SS's desatualizadas.");
			gerarNovas = false;
			gerarPendentes = false;
			gerarAntigas = false;
			return;
		}

		if (b == novasBatch) {
			configuraEscopo(new String[] { "NOVAS", "BATCH" });
			return;
		}

		if (b == novasOnline) {
			configuraEscopo(new String[] { "NOVAS", "ONLINE" });
			return;
		}
		if (b == pendentes) {
			configuraEscopo(new String[] { "PENDENTES" });
			return;
		}
		throw new RuntimeException("Erro interno");

	}

	private static int contarPendentes() {
		System.out.println("Contando SS's pendentes:");

		String sql = "select count(*) as desatualizados\r\n"
				+ mioloSQLPendentes();

		int r = singleIntQuery(sql);
		System.out.println(r + " registros.");
		return r;
	}

	private static String mioloSQLPendentes() {
		return "from dof_ss s\r\n"
				+ "left join dof_situacaoss t on t.id = s.situacao\r\n"
				+ "left join dof_estatisticas_ss e on s.situacao = e.codigo\r\n"
				+ "left join dof_motivocancelamentoss mc on s.motivocancel = mc.id\r\n"
				+ "where situacao not in (2)\r\n"
				+ "  and (now() - s.updated) <= cast(trunc(10 * t.fatorprazo * e.fatorprazo) || ' days' as interval)\r\n"
				+ "  and not (mc.baixarssnovamente = false)";
	}

	private static int estimarNovasBatch() {
		System.out.println("Estimando SS's novas batch em atraso:");
		String sql = "select  extract(day from now() - (solicitacao + interval '"
				+ latencia
				+ " days')) * 6000 as desatualizados      \r\n"
				+ "from dof_ss\r\n"
				+ "where numero > 800000000\r\n"
				+ "order by numero desc\r\n" + "limit 1";
		int r = singleIntQuery(sql);
		System.out.println(r + " registros.");
		return r;
	}

	private static int estimarNovasOnline() {
		System.out.println("Estimando SS's novas online em atraso:");
		String sql = "select  extract(day from now() - (solicitacao + interval '"
				+ latencia
				+ " days')) * 6000 as desatualizados      \r\n"
				+ "from dof_ss\r\n"
				+ "where numero < 800000000\r\n"
				+ "order by numero desc\r\n" + "limit 1";
		int r = singleIntQuery(sql);
		System.out.println(r + " registros.");
		return r;
	}

	private static int singleIntQuery(String sql) {
		ResultSet rs = query(sql);
		try {
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static int biggest(int... is) {
		int r = is[0];
		for (int i = 1; i < is.length; i++) {
			if (is[i] > r)
				r = is[i];
		}
		return r;
	}

	private static boolean contains(String[] args, String a) {
		for (String s : args) {

			if (s.equalsIgnoreCase(a))
				return true;
		}
		return false;
	}

	private static void gerarNovas() {
		try {
			if (gerarBatch)
				gerarNovasSeqBatch();
			if (gerarOnline)
				gerarNovasSeqOnline();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static void gerarAntigas() {
		try {
			if (gerarBatch)
				gerarAntigasSeqBatch();
			if (gerarOnline)
				gerarAntigasSeqOnline();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static void gerarNovasSeqOnline() throws IOException, SQLException {
		// ResultSet rs =
		// query("select numero, (select numero from dof_ss s2 where s2.numero > s1.numero order by numero limit 1) as proximo\r\n"
		// + "from dof_ss s1\r\n"
		// + "where numero >= 599109165 and abs(numero - "
		// +
		// "(select numero from dof_ss s2 where s2.numero > s1.numero order by numero limit 1)) > 100\r\n");

		ResultSet rs = query("select numero, 800000000 as proximo from dof_ss"
				+ " where numero < 800000000 order by numero desc limit 1");

		rs.next();
		long num = rs.getLong("numero");
		long prox = rs.getLong("proximo");
		File f = createFile(currentDir + "\\novas-online-" + num + ".src");
		System.out.println("Gerando arquivo " + f.getAbsolutePath());
		BufferedWriter w = new BufferedWriter(new FileWriter(f));
		String s = num + "~" + prox;
		w.write(s + "\r\n");
		w.close();
	}

	private static File createFile(String fileName) throws IOException {
		File f = new File(fileName);
		if (!f.exists())
			f.createNewFile();
		return f;
	}

	private static void gerarNovasSeqBatch() throws IOException, SQLException {
		ResultSet rs = query("select numero from dof_ss where numero >= 800000000 order by numero desc limit 1");
		rs.next();
		long num = rs.getLong("numero");
		File f = createFile(currentDir + "\\novas-batch-" + num + ".src");
		System.out.println("Gerando arquivo " + f.getAbsolutePath());
		BufferedWriter w = new BufferedWriter(new FileWriter(f));
		String s = num + "~" + (num + 10000000);
		w.write(s + "\r\n");
		w.close();
	}

	private static void gerarSSPendentes() {
		ResultSet rs = seekPendingSS();
		gerarArquivos(rs);
		System.out.println("Total de SS geradas: " + totalGeradas);
	}

	private static void gerarArquivos(ResultSet rs) {
		try {
			while (rs.next()) {
				int numero = rs.getInt("numero");
				if (totalGeradas > LIMITE_SS_PENDENTES)
					return;
				exportarSSParaArquivo(numero);
			}
			if (writer != null)
				writer.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static void exportarSSParaArquivo(long numero) throws IOException {
		if (!estaNoEscopoRelBatchOnline(numero))
			return;

		if (gerarNovoArquivo()) {
			// double x = numero / (LINHAS_POR_ARQUIVO * 10);
			// base = Math.round(Math.floor(x) * (LINHAS_POR_ARQUIVO * 10));
			if (base == null)
				base = (long) 1;
			else
				base++;

			if (writer != null) {
				writer.close();
				writer = null;
			}

			NumberFormat formatter = new DecimalFormat("00000");
			String sbase = formatter.format(base);
			String fileName = currentDir + "/" + sbase + "-" + numero + ".src";
			System.out.println("Gerando arquivo " + fileName);
			File f = new File(fileName);
			if (!f.exists())
				f.createNewFile();
			writer = new BufferedWriter(new FileWriter(f));
			linhasNoArquivo = 0;
		}

		writer.append(numero + "\r\n");
		totalGeradas++;
		linhasNoArquivo++;

	}

	private static boolean estaNoEscopoRelBatchOnline(long numero) {
		if (numero > LIMIAR_NUMERACAO_BATCH_ONLINE) {
			// é batch
			if (gerarBatch)
				return true;
		} else {
			// é online
			if (gerarOnline)
				return true;
		}
		return false;
	}

	private static String getWorkingDir() {
		File dir = new File(workingDir);
		if (dir.exists())
			return workingDir;
		return workingDirOld;
	}

	private static boolean gerarNovoArquivo() {
		if (base == null)
			return true;
		if (linhasNoArquivo >= LINHAS_POR_ARQUIVO)
			return true;
		return false;
	}

	private static ResultSet seekPendingSS() {
		// String sql = "select s.numero as numero\r\n"
		// + "from dof_ss s\r\n"
		// + "left join dof_situacaoss t on t.id = s.situacao\r\n"
		// + "left join dof_estatisticas_ss e on s.situacao = e.codigo\r\n"
		// + "where not situacao in (2)\r\n"
		// + "  and date_trunc('day', s.updated) < now() - cast(trunc("
		// + latencia
		// + "* t.fatorprazo * e.fatorprazo ) || ' days' as interval)\r\n"
		// + "order by t.descricao desc,\r\n"
		// + "date_trunc('day', s.updated), date_trunc('day', solicitacao)";

		String sql = "select s.numero as numero\r\n"
				+ mioloSQLPendentes()
				+ "order by t.descricao desc,\r\n"
				+ "date_trunc('day', s.updated), date_trunc('day', solicitacao)";

		return query(sql);
	}

	protected static void execSQL(String sql) {

		try {
			Statement s = connection.createStatement();
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static ResultSet query(String sql) {
		try {
			Statement s = connection.createStatement();
			return s.executeQuery(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private static void gerarAntigasSeqOnline() throws IOException,
			SQLException {
		// ResultSet rs =
		// query("select numero from dof_ss order by numero limit 1");
		ResultSet rs = query("select least(ult_lanc, inicio_buraco) as numero\r\n"
				+ "from (	select ult_lanc, (select inicio from dof_buraco_ss where ult_lanc > inicio and ult_lanc <= final) as inicio_buraco\r\n"
				+ "from (select numero as ult_lanc from dof_ss order by numero limit 1) x ) y");
		rs.next();
		long num = rs.getLong("numero");
		File f = createFile(currentDir + "\\antigas-online-" + num + ".src");
		System.out.println("Gerando arquivo " + f.getAbsolutePath());
		BufferedWriter w = new BufferedWriter(new FileWriter(f));
		String s = "~" + num;
		w.write(s + "\r\n");
		w.close();
	}

	private static void gerarAntigasSeqBatch() throws IOException, SQLException {
		long n = LIMIAR_NUMERACAO_BATCH_ONLINE;
		ResultSet rs = query("select numero from dof_ss where numero > " + n
				+ " order by numero limit 1");
		rs.next();
		long num = rs.getLong("numero");
		File f = createFile(currentDir + "\\antigas-batch-" + num + ".src");
		System.out.println("Gerando arquivo " + f.getAbsolutePath());
		BufferedWriter w = new BufferedWriter(new FileWriter(f));
		String s = "~" + num;
		w.write(s + "\r\n");
		w.close();
	}

}
