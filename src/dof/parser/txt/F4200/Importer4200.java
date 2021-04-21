package dof.parser.txt.F4200;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import dof.util.SQL;

public class Importer4200 {
	
	Connection connection;
	private CIAF4200Parser parser;
	

	public Importer4200(CIAF4200Parser parser, Connection connection) {
		super();
		this.connection = connection;
		this.parser = parser;
	}


	String assignParameters(String sql, Row4200 row) {
		sql = sql.replace("%mat", SQL.fieldValue(row.matricula));
		sql = sql.replace("%ano", SQL.fieldValue(row.ano));
		sql = sql.replace("%mes", SQL.fieldValue(row.mes));
		sql = sql.replace("%leit", SQL.fieldValue(row.leitura));
		sql = sql.replace("%anl", SQL.fieldValue(row.anl));
		sql = sql.replace("%cons", SQL.fieldValue(row.consumo));
		sql = sql.replace("%anc", SQL.fieldValue(row.anc));
		sql = sql.replace("%dias", SQL.fieldValue(row.diasConsumo));
		sql = sql.replace("%vfat", SQL.fieldValue(row.volumeFaturado));
		sql = sql.replace("%agua", SQL.fieldValue(row.valorAgua));
		sql = sql.replace("%esg", SQL.fieldValue(row.valorEsgoto));
		sql = sql.replace("%serv", SQL.fieldValue(row.valorServicos));
		sql = sql.replace("%baixa", SQL.fieldValue(row.baixa));

		String t = SQL.timestampFieldValue(parser.fileLastModifiedDate());
		sql = sql.replace("%updl", t);
		sql = sql.replace("%updf", t);
		sql = sql.replace("%updb", t);
		return sql;
	}


	String assignParametersAtualizacaoFatura(String sql, Row4200 row) {
		sql = sql.replace("%mat", SQL.fieldValue(row.matricula));
		sql = sql.replace("%ano", SQL.fieldValue(row.anoAnt));
		sql = sql.replace("%fats", SQL.fieldValue(row.getFaturasAnoAtual()));
		sql = sql.replace("%fbcn", SQL.fieldValue(row.getFaturasBCNAnoAtual()));
		sql = sql.replace("%upd", SQL.timestampFieldValue(parser.fileLastModifiedDate()));
		return sql;
	}


	protected void registraFatura(Row4200 row) {

		try {
			Statement s = connection.createStatement();

			String sql = "update dof_fatura " + "set leitura = %leit, cod_anl = %anl, "
					+ "consumo = %cons, cod_anc = %anc, "
					+ "dias_consumo = %dias, volume_faturado = %vfat, "
					+ "agua = %agua, esgoto = %esg, servicos = %serv,  " + "baixa = %baixa, "
					+ "updatedleitura = %updl, updatedfatura = %updf, updatedbaixa = %updb "
					+ "where matricula = %mat and mes = %mes and ano = %ano";
			sql = assignParameters(sql, row);
			s.execute(sql);
			int u = s.getUpdateCount();
			if (u > 0)
				return;

			sql = "insert into dof_fatura (matricula, ano, mes, leitura, cod_anl, consumo, cod_anc,"
					+ "dias_consumo, volume_faturado, agua, esgoto, servicos, baixa, "
					+ "updatedleitura, updatedfatura, updatedbaixa) "
					+ "values (%mat, %ano, %mes, %leit, %anl, %cons, %anc, %dias, %vfat, "
					+ "%agua, %esg, %serv, %baixa, %updl, %updf, %updb);";
			sql = assignParameters(sql, row);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	protected void deletaFatura(Row4200 row) {

		try {
			Statement s = connection.createStatement();

			String sql = "delete from dof_fatura "
					+ "where matricula = %mat and mes = %mes and ano = %ano;";
			sql = assignParameters(sql, row);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}


	public void doImport(Row4200 row) {
		if (row.registroValido())
			registraFatura(row);
		else
			deletaFatura(row);
	}

	protected void registraAtualizacaoFaturaAnoRegAnt(Row4200 row) {
		try {
			Statement s = connection.createStatement();

			String sql = "update dof_atualizacaofatura set updated = %upd, faturas = %fats,"
					+ " faturasbcn = %fbcn \r\n"
					+ "where matricula = %mat and ano = %ano;";
			sql = assignParametersAtualizacaoFatura(sql, row);

			s.execute(sql);
			int u = s.getUpdateCount();
			if (u > 0)
				return;

			sql = "insert into dof_atualizacaofatura (matricula, ano, faturas, updated, faturasbcn) "
					+ "values (%mat, %ano, %fats, %upd, %fbcn);";
			sql = assignParametersAtualizacaoFatura(sql, row);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}


}
