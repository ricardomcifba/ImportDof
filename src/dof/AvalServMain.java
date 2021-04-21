package dof;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

import dof.export.TXTExporter;
import dof.util.FCFP;

public class AvalServMain {

	private static String escopo;
	private static Integer ano;
	private static String dataAtual;
	private static Integer localidade;
	// private static Integer setor;

	private static Connection connection;
	private static Scanner keyboardScanner;

	private static TXTExporter fileExporter;
	private static String outputFileName;
	private static String servicos;
	private static String filterLoc = "";

	public static void main(String[] args) throws IOException, SQLException {
		// System.out.println(getCurrentDir());
		System.out.println("Geracao de dados de Avaliacao dos Servicos");
		dataAtual = getDataAtual();

		System.out.println(getTime() + " Conectando a base de dados...");
		// connection =
		// DriverManager.getConnection("jdbc:postgresql://10.19.152.47:5432/fcfdof",
		// "postgres", "fcfp01");
		connection = FCFP.newConnectionFromFile("db.conf");

		escopo = inputQuery("Digite o escopo (DM/DN/DS/UR): ");
		ano = null;

		do {
			String sano = inputQuery("Digite o ano de execucao: ");
			try {
				ano = Integer.parseInt(sano);
			} catch (NumberFormatException e) {
			}

			if (ano == null)
				System.out.println("Ano invalido.");
		} while (ano == null);

		int[] arrayserv = null;
		String slistaserv = "";

		do {
			servicos = inputQuery("Digite os tipos de servico [ENTER=1,2,3,4,5,6,7,8,9]: ");

			if ((servicos == null) || (servicos.trim().equals("")))
				servicos = "1,2,3,4,5,6,7,8,9";

			slistaserv = "S" + servicos.replace(",", "");

			try {
				arrayserv = intArrayFromByComma(servicos);

				// if (arrayserv != null)
				// if (arrayserv.length == 0)
				// arrayserv = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };

			} catch (NumberFormatException e) {
				arrayserv = null;
				System.out.println("Código numérico inválido.");
			}

			for (int i = 0; i < arrayserv.length; i++) {
				int s = arrayserv[i];
				if ((s < 1) || (s > 9)) {
					System.out.println("Tipo de serviço inválido: " + s);
					arrayserv = null;
					break;
				}
			}

		} while (arrayserv == null);

		String sfilterloc = inputQuery("Filtrar localidades (ENTER = Nao)? ");
		if (sfilterloc != null)
			if (!sfilterloc.trim().equals(""))
				filterLoc = "localidade " + sfilterloc;

		if (slistaserv.equals("S123456789"))
			slistaserv = "";

		String reproc = inputQuery("Deseja recalcular ganhos (S/N)? Isto pode ser demorado: ");

		boolean calc = matchString(reproc, "S", "s");

		outputFileName = "RESUMO " + escopo + " " + ano + " em " + dataAtual + slistaserv + ".txt";
		perform(calc);

		System.out.println(getTime() + " Concluído.");

	}

	private static int[] intArrayFromByComma(String servicos) {
		String[] ss = servicos.split(",");
		int[] r = new int[ss.length];
		for (int i = 0; i < r.length; i++) {
			r[i] = Integer.parseInt(ss[i]);
		}
		return r;
	}

	private static String getTime() {
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
		String s = sdf.format(new Date());
		return "[" + s + "]";
	}

	private static void perform(boolean calc) throws SQLException, IOException {
		System.out.println(getTime() + " Consultando localidades/setores");
		// ResultSet rsl = queryLocalSetor();
		ResultSet rsl = queryLocalidades();
		// boolean exportedHeader = false;

		fileExporter = new TXTExporter(outputFileName);
		boolean exportedHeader = false;

		while (rsl.next()) {
			localidade = rsl.getInt("localidade");
			// setor = rsl.getInt("setor");
			if (calc) {
				// System.out.print(getTime() + " Recalculando ganhos para L" +
				// localidade + " S" + setor + ": ");
				System.out.print(getTime() + " Recalculando ganhos para L" + localidade);
				reprocessaGanhosLocalidadeAtual();
			}
			System.out.println(getTime() + " Consultando resumo para L" + localidade);

			ResultSet rsc = queryResumo();

			System.out.println(getTime() + " Exportando para o arquivo " + outputFileName);
			if (!exportedHeader) {
				fileExporter.exportHeader(rsc);
				exportedHeader = true;
			}

			fileExporter.exportData(rsc);
		}

		fileExporter.close();

	}

	static String getCurrentDir() {
		return System.getProperty("user.dir");
	}

	private static String getDataAtual() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		return sdf.format(new Date());
	}

	static ResultSet queryLocalSetor() throws SQLException {
		String sql = "select localidade, setor from view_dof_setor\r\n"
				+ "  where sigla_diretoria in (@escopo)\r\n"
				+ "  order by diretoria, localidade, setor;\r\n";
		sql = replaceParameters(sql);
		ResultSet rs = query(sql);
		return rs;
	}

	private static ResultSet queryLocalidades() throws SQLException {
		// String sql = "select localidade from view_dof_localidade\r\n"
		// + "  where sigla_diretoria in (@escopo)\r\n"
		// + "  order by diretoria, localidade;\r\n";
		String sql = "select localidades(@escopo) as localidade";
		sql = replaceParameters(sql);

		if (!filterLoc.equals(""))
			sql = "select localidade from (" + sql + ") x where " + filterLoc;

		sql = sql + ";";
		ResultSet rs = query(sql);
		return rs;
	}

	private static String replaceParameters(String sql) {
		String r = sql;
		r = r.replace("@escopo", fieldValue(escopo));
		r = r.replace("@ano", fieldValue(ano));
		r = r.replace("@localidade", fieldValue(localidade));
		// r = r.replace("@setor", fieldValue(setor));
		r = r.replace("@servicos", fieldValueAsNative(servicos));
		return r;
	}

	private static CharSequence fieldValue(Integer value) {
		if (value == null)
			return "null";

		return value.toString();
	}

	private static String fieldValue(String s) {
		if (s == null)
			return "null";

		s = s.replace("'", "''");
		return "'" + s + "'";
	}

	private static String fieldValueAsNative(String s) {
		if (s == null)
			return "null";

		return s;
	}

	private static void reprocessaGanhosLocalidadeAtual() throws SQLException {
		int total = 0;
		String sql = "select amf_processa_ganho_exec_servico(@ano, v.matricula) \r\n"
				+ "  from amf_resumo_trabalhadas(array[@servicos], @escopo, @ano) v \r\n" 
				+ "  join dof_usuario u on v.matricula = u.matricula \r\n"
				+ "  where v.localidade = @localidade\r\n" + "    and u.localidade = @localidade;";
		sql = replaceParameters(sql);
		ResultSet rs = query(sql);
		while (rs.next())
			total++;
		System.out.println(" - " + total + " matrículas atualizadas.");
	}

	private static ResultSet query(String sql) throws SQLException {
		try {
			// System.out.println(sql);
			Statement s = connection.createStatement();
			ResultSet r = s.executeQuery(sql);
			return r;
		} catch (SQLException e) {
			System.out.println(e.getMessage() + " em:");
			System.out.println(sql);
			throw e;
		}
	}

	private static boolean matchString(String s, String... matchList) {
		for (String m : matchList) {
			if (s.equals(m))
				return true;
		}
		return false;
	}

	private static String inputQuery(String msg) {
		if (keyboardScanner == null)
			keyboardScanner = new Scanner(System.in);

		System.out.print(msg);
		return keyboardScanner.nextLine();
	}

	private static ResultSet queryResumo() throws SQLException, IOException {
		String sql = loadContentFromFile("avalserv.sql");
		sql = replaceParameters(sql);
		return query(sql);
	}

	private static String loadContentFromFile(String filePath) throws IOException {

		String r = null;
		BufferedReader in = new BufferedReader(new FileReader(filePath));
		try {
			while (in.ready()) {
				String s = in.readLine();
				if (r != null)
					r += "\r\n" + s;
				else
					r = s;
			}
		} finally {
			in.close();
		}
		return r;
	}

}
