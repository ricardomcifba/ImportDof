package dof.bridge.view4200;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import dof.util.SQL;
import dof.util.Util;

public class Default4200Bridge extends Abstract4200Bridge {

	private static final String FINALIDADE_OBSOLETOS = "OBSOLETOS";

	private static final String FINALIDADE_AVAL_SERV = "AVAL_SERV";

	private String finalidade;

	private ArrayList<String> escopos = new ArrayList<String>();

	private ArrayList<Integer> anos = new ArrayList<Integer>();

	private Integer qtdeMaxMatriculas = 1000;

	public Default4200Bridge(Connection source, Connection target) {
		super(source, target);
	}

	@Override
	protected String getFilter() {
		String r = "";

		r = "where " + filterFinalidade() + " and ano in (" + anosByComma() + ");";

		return r;
	}

	@Override
	protected boolean shouldRepeat(int lastQueryRows, int totalRows) {
		if (totalRows > 10000000)
			return false;
		if (lastQueryRows == 0)
			return false;
		return true;
	}

	private static String[] escoposGerenciados = { "DM", "DN", "DS", "UMS", "UMC", "UMF", "UML",
			"UMB", "UMJ", "UNA", "UNB", "UNF", "UNI", "UNE", "UNP", "UNS", "USC", "USI", "USU",
			"USJ", "USA", "USV" };

	private static String[] finalidadesGerenciadas = { FINALIDADE_AVAL_SERV, FINALIDADE_OBSOLETOS };

	private static boolean isEscopo(String s) {
		if (Util.contains(escoposGerenciados, s))
			return true;
		if (isLocalidade(s))
			return true;
		return false;
	}

	private static boolean isLocalidade(String p) {
		try {
			int l = Integer.parseInt(p);

			if (l < 1500)
				return true;

		} catch (NumberFormatException e) {
			return false;
		}

		return false;
	}

	@Override
	public void assignSpecificParams(String[] params) {
		for (String p : params) {
			if (isAno(p))
				anos.add(Integer.parseInt(p));

			if (isEscopo(p))
				escopos.add(p);

			if (Util.contains(finalidadesGerenciadas, p))
				assignCriterio(p);

			if (isNoDiasDesatualizacao(p))
				assignNoDiasDesatualizacao(p);
		}
	}

	private void assignNoDiasDesatualizacao(String p) {
		int nd = parseNoDiasDesatualizacao(p);
		setNoDiasDesatualizacao(nd);
	}

	private int parseNoDiasDesatualizacao(String p) {
		if (p.equals("1day"))
			return 1;
		p = p.replace("days", "");
		return Integer.parseInt(p);
	}

	private boolean isNoDiasDesatualizacao(String p) {
		return p.contains("days") || p.equals("1day");
	}

	private boolean isAno(String p) {
		try {
			int ano = Integer.parseInt(p);

			if (ano < 1980)
				return false;

			if (ano > 2030)
				return false;

		} catch (NumberFormatException e) {
			return false;
		}

		return true;
	}

	private void assignCriterio(String finalidade) {
		if (this.finalidade != null)
			throw new RuntimeException("Duplicidade de finalidade: " + this.finalidade + " e "
					+ finalidade);
		this.setFinalidade(finalidade);
	}

	protected String filterFinalidade() {
		String sql = sqlTargetFinalidade();
		try {
			return "matricula in (" + listMatriculasInTargetByComma(sql) + ")";
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private String listMatriculasInTargetByComma(String sql) throws SQLException, IOException {
		ResultSet rs = SQL.query(target, sql);
		String r = "";
		while (rs.next()) {
			String s = rs.getLong("matricula") + "";
			if (!r.equals(""))
				r += ",";
			r += s;
		}
		return r;
	}

	private String sqlTargetFinalidade() {
		if (finalidade == null)
			return "";
		if (finalidade.equals(FINALIDADE_AVAL_SERV))
			return sqlAvalServInTarget();
		if (finalidade.equals(FINALIDADE_OBSOLETOS))
			return sqlObsoletosInTarget();
		throw new RuntimeException("Finalidade desconhecida: " + getFinalidade());
	}

	private String sqlObsoletosInTarget() {
		String filterLoc = "";
		if (escopos.size() > 0)
			filterLoc = "  and u.localidade in (" + filterSubselectLocalidades() + ")\r\n";

		String r = "select distinct a.ano, u.matricula\r\nfrom dof_usuario u\r\n"
				+ "left join dof_atualizacaofatura a on u.matricula = a.matricula\r\n"
				+ "where a.ano in (%anos)\r\n" + filterLoc
				+ "  and (a.updated is null or a.updated < now() - interval '%diasdes days')\r\n"
				+ "  order by a.ano desc, u.matricula\r\nlimit %limit;";
		r = r.replace("%anos", anosByComma());
		r = r.replace("%diasdes", noDiasDesatualizacao + "");
		r = r.replace("%limit", qtdeMaxMatriculas + "");
		return r;
	}

	private String anosByComma() {
		String r = "";
		for (Integer a : anos) {
			if (!r.isEmpty())
				r += ",";
			r += a;
		}
		return r;
	}

	private String sqlAvalServInTarget() {
		String filterLoc = "";
		if (escopos.size() > 0)
			filterLoc = "  and t.localidade in (" + filterSubselectLocalidades() + ")\r\n";

		String r = "select distinct t.matricula\r\nfrom view_amf_resumotrabalhadas t\r\n"
				+ "left join dof_atualizacaofatura a on t.matricula = a.matricula\r\n"
				+ "where t.data_execucao between '%anoini-1-1' and '%anofim-12-31'\r\n"
				+ "  and a.ano between %anoini - 1 and date_part('year', now() - interval '2 months')\r\n"
				+ filterLoc
				+ "  and (a.updated is null or a.updated < now() - interval '%diasdes days')\r\n"
				+ "  order by ano desc, matricula\r\nlimit %limit;";
		r = r.replace("%anoini", menorAno() + "");
		r = r.replace("%anofim", maiorAno() + "");
		r = r.replace("%diasdes", noDiasDesatualizacao + "");
		r = r.replace("%limit", qtdeMaxMatriculas + "");
		return r;
	}

	private String filterSubselectLocalidades() {
		String r = "";
		for (String s : escopos) {
			if (!r.equals(""))
				r += " union ";
			String t = "select localidades('" + s + "')";
			r += t;
		}
		return r;
	}

	private int menorAno() {
		if (anos.size() == 0)
			throw new RuntimeException("Não foi definido nenhum ano de trabalho");
		int r = anos.get(0);
		for (int i = 1; i < anos.size(); i++) {
			int a = anos.get(i);
			if (a < r)
				r = a;
		}
		return r;
	}

	private int maiorAno() {
		if (anos.size() == 0)
			throw new RuntimeException("Não foi definido nenhum ano de trabalho");
		int r = anos.get(0);
		for (int i = 1; i < anos.size(); i++) {
			int a = anos.get(i);
			if (a > r)
				r = a;
		}
		return r;
	}

	public Integer getQtdeMaxMatriculas() {
		return qtdeMaxMatriculas;
	}

	public void setQtdeMaxMatriculas(Integer qtdeMaxMatriculas) {
		this.qtdeMaxMatriculas = qtdeMaxMatriculas;
	}

	public String getFinalidade() {
		return finalidade;
	}

	public void setFinalidade(String finalidade) {
		if (finalidade.equals(FINALIDADE_AVAL_SERV))
			setNoDiasDesatualizacao(7);
		if (finalidade.equals(FINALIDADE_OBSOLETOS))
			setNoDiasDesatualizacao(60);
		this.finalidade = finalidade;
	}
}
