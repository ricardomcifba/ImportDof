package dof.bridge.view4200;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import dof.bridge.Bridge;
import dof.util.SQL;

public abstract class Abstract4200Bridge extends Bridge {

	private Integer matricula;
	private Integer ano;

	private Integer anoAnt;
	private int faturasAnoAtual = 0;
	private int faturasBCNAnoAtual = 0;

	private Integer mes;
	private Integer leitura;
	private Integer anl;
	private Integer consumo;
	private String anc;

	private Integer diasConsumo;
	private Integer volumeFaturado;
	private Double valorAgua;
	private Double valorEsgoto;
	private Double valorServicos;
	private String baixa;
	private Date updated;
	private boolean newLine = true;

	protected int noDiasDesatualizacao = 7;

	public Abstract4200Bridge(Connection source, Connection target) {
		super(source, target);
	}

	@Override
	protected void processRow(int rowId, ResultSet rs) throws SQLException {
		// super.processRow(rowId, rs);

		extractRow(rowId, rs);

		if (registroValido())
			registraFatura();
		else
			deletaFatura();
		// System.out.println(this.toString());
	}

	private boolean registroValido() {
		throw new RuntimeException("ainda não implementado");
		// TODO implementar, considerando se virá ou não faturas nulas.
		// Se vier, basta identificar se é válido ou não pelo conteúdo da linha
		// Se não vier, tem que avaliar do que foi solicitado, o que veio,
		// e remover a diferença

		// if ((valorAgua == null) && (valorEsgoto == null)
		// && (valorServicos == null) && (consumo == null)
		// && (anl == null) && (leitura == null))
		// return false;
		// return true;
	}

	private void extractRow(int rowId, ResultSet rs) throws SQLException {
		print(globalRowId);
		print(rowId);
		matricula = rs.getInt("matricula");
		print(matricula);
		ano = rs.getInt("ano");
		print(ano);
		
		faturasAnoAtual++;
		if (BCN(baixa)) {
			faturasBCNAnoAtual++;
		}
		if (!ano.equals(anoAnt)) {
			mudouAno();
			anoAnt = ano;
			faturasAnoAtual = 0;
			faturasBCNAnoAtual = 0;
		}

		mes = rs.getInt("mes");
		print(mes);
		leitura = rs.getInt("leitura");
		print(leitura);
		anl = rs.getInt("anl");
		print(anl);
		consumo = rs.getInt("consumo");
		print(consumo);
		anc = rs.getString("anc");
		print(anc);
		diasConsumo = rs.getInt("diasconsumo");
		print(diasConsumo);
		volumeFaturado = rs.getInt("volumefaturado");
		print(volumeFaturado);
		valorAgua = rs.getDouble("valoragua");
		print(valorAgua);

		if ((volumeFaturado != null) && (volumeFaturado != null))
			if ((volumeFaturado > 0) && (valorAgua == 0.0)) {
				System.out.println("Aviso: volume faturado " + volumeFaturado
						+ " sem valor de água. Corrigindo para volume faturado zero.");
				volumeFaturado = 0;
			}
		valorEsgoto = rs.getDouble("valoresgoto");
		print(valorEsgoto);
		valorServicos = rs.getDouble("valorservicos");
		print(valorServicos);
		baixa = rs.getString("baixa");
		print(baixa);
		updated = rs.getDate("updated");
		print(updated);
		println();
	}

	private boolean BCN(String b) {
		if (b.equals("B"))
			return true;
		if (b.equals("C"))
			return true;
		if (b.equals("N"))
			return true;
		return false;
	}

	private void mudouAno() {
//		if (anoAnt != null)
//			registraAtualizacaoFaturaAnoRegAnt();
	}

	private void registraAtualizacaoFaturaAnoRegAnt() {
		try {
			Statement s = target.createStatement();

			String sql = "update dof_atualizacaofatura set updated = %upd, faturas = %fats, \r\n"
					+ "faturasbcn = %fbcn where matricula = %mat and ano = %ano;";
			sql = assignParametersAtualizacaoFatura(sql);

			s.execute(sql);
			int u = s.getUpdateCount();
			if (u > 0)
				return;

			sql = "insert into dof_atualizacaofatura (matricula, ano, faturas, updated, faturasbcn) "
					+ "values (%mat, %ano, %fats, %upd, %fbcn);";
			sql = assignParametersAtualizacaoFatura(sql);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private String assignParametersAtualizacaoFatura(String sql) {
		sql = sql.replace("%mat", SQL.fieldValue(matricula));
		sql = sql.replace("%ano", SQL.fieldValue(anoAnt));
		sql = sql.replace("%fats", SQL.fieldValue(faturasAnoAtual));
		sql = sql.replace("%fbcn", SQL.fieldValue(faturasBCNAnoAtual));
		sql = sql.replace("%upd", SQL.timestampFieldValue(updated));
		return sql;
	}

	private void println() {
		System.out.println();
		newLine = true;
	}

	private void print(Object content) {
		if (!newLine)
			System.out.print("|");
		System.out.print(content);
		newLine = false;
	}

	protected void registraFatura() {

		try {
			Statement s = target.createStatement();

			String sql = "update dof_fatura " + "set leitura = %leit, cod_anl = %anl, "
					+ "consumo = %cons, cod_anc = %anc, "
					+ "dias_consumo = %dias, volume_faturado = %vfat, "
					+ "agua = %agua, esgoto = %esg, servicos = %serv,  " + "baixa = %baixa, "
					+ "updatedleitura = %updl, updatedfatura = %updf, updatedbaixa = %updb "
					+ "where matricula = %mat and mes = %mes and ano = %ano";
			sql = assignParameters(sql);
			s.execute(sql);
			int u = s.getUpdateCount();
			if (u > 0)
				return;

			sql = "insert into dof_fatura (matricula, ano, mes, leitura, cod_anl, consumo, cod_anc,"
					+ "dias_consumo, volume_faturado, agua, esgoto, servicos, baixa, "
					+ "updatedleitura, updatedfatura, updatedbaixa) "
					+ "values (%mat, %ano, %mes, %leit, %anl, %cons, %anc, %dias, %vfat, "
					+ "%agua, %esg, %serv, %baixa, %updl, %updf, %updb);";
			sql = assignParameters(sql);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	protected void deletaFatura() {

		try {
			Statement s = target.createStatement();

			String sql = "delete from dof_fatura "
					+ "where matricula = %mat and mes = %mes and ano = %ano;";
			sql = assignParameters(sql);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private String assignParameters(String sql) {
		sql = sql.replace("%mat", SQL.fieldValue(matricula));
		sql = sql.replace("%ano", SQL.fieldValue(ano));
		sql = sql.replace("%mes", SQL.fieldValue(mes));
		sql = sql.replace("%leit", SQL.fieldValue(leitura));
		sql = sql.replace("%anl", SQL.fieldValue(anl));
		sql = sql.replace("%cons", SQL.fieldValue(consumo));
		sql = sql.replace("%anc", SQL.fieldValue(anc));
		sql = sql.replace("%dias", SQL.fieldValue(diasConsumo));
		sql = sql.replace("%vfat", SQL.fieldValue(volumeFaturado));
		sql = sql.replace("%agua", SQL.fieldValue(valorAgua));
		sql = sql.replace("%esg", SQL.fieldValue(valorEsgoto));
		sql = sql.replace("%serv", SQL.fieldValue(valorServicos));
		sql = sql.replace("%baixa", SQL.fieldValue(baixa));

		String t = SQL.timestampFieldValue(updated);
		sql = sql.replace("%updl", t);
		sql = sql.replace("%updf", t);
		sql = sql.replace("%updb", t);
		return sql;
	}

	@Override
	protected String getSourceSQL() {
		return "select * from tabela " + getFilter() + " order by matricula, ano, mes;";
	}

	protected abstract String getFilter();

	public int getNoDiasDesatualizacao() {
		return noDiasDesatualizacao;
	}

	public void setNoDiasDesatualizacao(int diasDesatualizacao) {
		this.noDiasDesatualizacao = diasDesatualizacao;
	}

}
