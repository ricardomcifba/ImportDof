\package dof.parser.txt.old;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import dof.parsers.Parser;

@Deprecated
public class CIAF4200ParserOld extends Parser {

	private Long matricula;
	private Integer ano;
	private Integer mes;
	private Integer leitura;
	private Integer anl;
	private Integer consumo;
	private String anc;

	public CIAF4200ParserOld(Connection connection, String fileName, String[] args) {
		super(connection, fileName, args);
	}

	@Override
	protected void processRow(int rowId, String content) {
		super.processRow(rowId, content);
		parseLancRow(content);
//		System.out.println(content);
		 registraConsumo();
//		System.out.println(this.toString());
	}

	@Override
	public String toString() {
		return "CIAF4200Parser [matricula=" + matricula + ", ano=" + ano
				+ ", mes=" + mes + ", leitura=" + leitura + ", anl=" + anl
				+ ", consumo=" + consumo + ", anc=" + anc + "]";
	}

	private void parseLancRow(String content) {
		String s = extract(content, 0, 10);
		matricula = valorLong(s);

		s = extract(content, 10, 4);
		ano = valorInt(s);

		s = extract(content, 14, 2);
		mes = valorInt(s);

		s = extract(content, 16, 6);
		leitura = valorInt(s);

		s = extract(content, 22, 2);
		anl = valorInt(s);

		s = extract(content, 24, 6);
		consumo = valorInt(s);

		s = extract(content, 30, 2);
		anc = s;

	}

	protected void registraConsumo() {
		
		try {
			Statement s = connection.createStatement();

			String sql = "update dof_fatura "
					+ "set leitura = %leit, cod_anl = %anl, "
					+ "consumo = %cons, cod_anc = %anc "
					+ "where matricula = %mat and mes = %mes and ano = %ano";
			sql = assignParameters(sql);
			s.execute(sql);
			int u = s.getUpdateCount();
			if (u > 0)
				return;

			sql = "insert into dof_fatura (matricula, ano, mes, leitura, cod_anl, consumo, cod_anc) "
					+ "values (%mat, %ano, %mes, %leit, %anl, %cons, %anc);";
			sql = assignParameters(sql);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	private String assignParameters(String sql) {
		sql = sql.replace("%mat", matricula + "");
		sql = sql.replace("%ano", ano + "");
		sql = sql.replace("%mes", mes + "");
		sql = sql.replace("%leit", leitura + "");
		sql = sql.replace("%anl", anl + "");
		sql = sql.replace("%cons", consumo + "");
		sql = sql.replace("%anc", "null");
		if (anc != null)
			if (!anc.trim().equals(""))
				sql = sql.replace("%anc", "'" + anc + "'");
		
		return sql;
	}

	@Override
	protected boolean rowContainsAllRequiredData(long rowId, String content) {
		return true;
	}

}
