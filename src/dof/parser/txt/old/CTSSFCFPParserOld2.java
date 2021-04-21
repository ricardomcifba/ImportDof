package dof.parser.txt.old;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import dof.parser.Parser;

@Deprecated
public class CTSSFCFPParserOld2 extends Parser {

	private long numSS;
	private Date dataHoraSol;
	private int sitSS;
	private Integer matUltMov;
	private Date dataUltMov;
	private Integer matFun;
	private Date dataInfCon;
	private Date dataReit;
	private long unidOrigem;
	private Integer numReit;
	private long unidAtual;
	private long unidFinal;
	private Long matricula;
	private Integer meioSol;
	private Integer codServ;
	private Integer motivoNExec;
	private Date prazo;
	private Double valor;
	private String observacao;
	private boolean layoutAntigo = false;

	public CTSSFCFPParserOld2(Connection connection, String fileName, String[] args) {
		super(connection, fileName, args);
		rowStartOffset = -1;

		String name = getSimpleFileName();
		if (name.startsWith("L1"))
			layoutAntigo = true;
	}

	@Override
	protected void processRow(int rowId, String content) {
		super.processRow(rowId, content);
		parseRow(content);
		importRow();
	}

	private void importRow() {
		// log("Atualizando fatura de " + matricula);

		try {
			Statement s = connection.createStatement();

			String sql = "update dof_ss set " + "codigo = %codigo, "
					+ "solicitacao = %solicitacao, " + "situacao = %situacao, "
					+ "matultimamov = %matultimamov, "
					+ "ultimamov = %ultimamov, "
					+ "matfuncionario = %matfuncionario, "
					+ "dataconclusao = %dataconclusao, "
					+ "datareiteracao = %datareiteracao, "
					+ "reiteracoes = %reiteracoes, "
					+ "unidorigem = %unidorigem, " + "unidatual = %unidatual, "
					+ "unidfinal = %unidfinal, " + "matricula = %matricula, "
					+ "meiosolicitacao = %meiosolicitacao, "
					+ "motivonexec = %motivonexec, " + "prazo = %prazo, "
					+ "valor = %valor, " + "observacao = %observacao, "
					+ "updated = %updated"
					+ " where numero = %numero;";
			sql = assignParametersSS(sql);
			// System.out.println(sql);
			s.execute(sql);
			int u = s.getUpdateCount();
			if (u > 0)
				return;

			sql = "insert into dof_ss (numero, codigo, solicitacao, situacao, matultimamov, ultimamov, matfuncionario, "
					+ "dataconclusao, datareiteracao, reiteracoes, unidorigem, unidatual, unidfinal, matricula, "
					+ "meiosolicitacao, motivonexec, prazo, valor, observacao, updated) "
					+ "values (%numero, %codigo, %solicitacao, %situacao, %matultimamov, %ultimamov, %matfuncionario, "
					+ "%dataconclusao, %datareiteracao, %reiteracoes, %unidorigem, %unidatual, %unidfinal, "
					+ "%matricula, %meiosolicitacao, %motivonexec, %prazo, %valor, %observacao, %updated);";
			sql = assignParametersSS(sql);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private String assignParametersSS(String sql) {
		sql = sql.replace("%numero", fieldValue(numSS));
		sql = sql.replace("%codigo", fieldValue(codServ));
		sql = sql.replace("%situacao", fieldValue(sitSS));
		sql = sql.replace("%solicitacao", timestampFieldValue(dataHoraSol));
		sql = sql.replace("%matultimamov", fieldValue(matUltMov));
		sql = sql.replace("%ultimamov", fieldValue(dataUltMov));
		sql = sql.replace("%matfuncionario", fieldValue(matFun));
		sql = sql.replace("%dataconclusao", fieldValue(dataInfCon));
		sql = sql.replace("%datareiteracao", fieldValue(dataReit));
		sql = sql.replace("%reiteracoes", fieldValue(numReit));
		sql = sql.replace("%unidorigem", fieldValue(unidOrigem));
		sql = sql.replace("%unidatual", fieldValue(unidAtual));
		sql = sql.replace("%unidfinal", fieldValue(unidFinal));
		sql = sql.replace("%matricula", fieldValue(matricula));
		sql = sql.replace("%meiosolicitacao", fieldValue(meioSol));
		sql = sql.replace("%motivonexec", fieldValue(motivoNExec));
		sql = sql.replace("%prazo", fieldValue(prazo));
		sql = sql.replace("%valor", fieldValue(valor));
		sql = sql.replace("%observacao", fieldValue(observacao));
		sql = sql.replace("%updated", timestampFieldValue(new Date()));
		return sql;
	}

	private void parseRow(String content) {
		if (content == null)
			return;
		String s = content.trim();

		numSS = valorLong(s, 1, 10);
		dataHoraSol = valorDateTime(s, 11, 12);
		sitSS = valorInt(s, 23, 1);
		matUltMov = valorInt(s, 24, 6);
		dataUltMov = valorDate(s, 30, 8);
		matFun = valorInt(s, 38, 6);
		dataInfCon = valorDate(s, 44, 8);
		dataReit = valorDate(s, 52, 8);
		numReit = valorInt(s, 72, 2);
		unidOrigem = valorLong(s, 60, 12);
		unidAtual = valorLong(s, 74, 12);
		unidFinal = valorLong(s, 86, 12);
		matricula = valorLong(s, 98, 9);
		meioSol = valorInt(s, 107, 1);

		codServ = valorInt(s, 108, 4);

		if (layoutAntigo) {

			motivoNExec = null; // bug do EBM TODO retificar e atualizar

			prazo = valorDate(s, 112, 8);
			valor = valorMonetario(s, 120, 16);
			observacao = valorString(s, 136, 400);
			if (observacao != null)
				observacao = observacao.trim();

		} else {
			motivoNExec = valorInt(s, 112, 2);

			prazo = valorDate(s, 114, 8);
			valor = valorMonetario(s, 122, 16);
			observacao = valorString(s, 138, 400);
			if (observacao != null)
				observacao = observacao.trim();

		}
	}

	private Date valorDateTime(String s, int pos, int tam) {
		String mask = "ddMMyyyyHHmm";
		return valorDateTime(s, pos, tam, mask);
	}

	private Date valorDateTime(String s, int pos, int tam, String mask) {
		s = extract(s, pos, tam);
		return valorDate(s, mask);
	}

	protected Double valorMonetario(String s, int pos, int tam) {
		s = extract(s, pos, tam);
		if (s != null)
			s = s.replace(".", "");
		return valorMonetario(s);
	}

	protected Long valorLong(String s, int ini, int tam) {
		String r = extract(s, ini, tam);
		return valorLong(r);
	}

	protected Date valorDate(String s, int ini, int tam, String mask) {
		String r = extract(s, ini, tam);
		if (r == null)
			return null;
		if (r.trim().equals(""))
			return null;
		if (r.equals("00000000"))
			return null;
		return valorDate(r, mask);
	}

	protected Date valorDate(String s, int ini, int tam) {
		return valorDate(s, ini, tam, "ddMMyyyy");
	}

	protected Date valorDate(String s, String mask) {
		SimpleDateFormat sdf = new SimpleDateFormat(mask);
		try {
			return sdf.parse(s);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	protected Integer valorInt(String s, int ini, int tam) {
		String r = extract(s, ini, tam);
		return valorInt(r);
	}

	protected String valorString(String s, int ini, int tam, boolean trim) {
		String r = extract(s, ini, tam);
		if (r == null)
			return null;
		if (trim)
			r = r.trim();
		return r;
	}

	protected String valorString(String s, int ini, int tam) {
		return valorString(s, ini, tam, true);
	}

	@Override
	protected boolean rowContainsAllRequiredData(long rowId, String content) {
		return true;
	}

}
