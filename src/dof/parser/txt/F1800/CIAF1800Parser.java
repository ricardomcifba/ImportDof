package dof.parser.txt.F1800;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;

import dof.parser.Categoria;
import dof.parser.Leitura;
import dof.parser.Localidade;
import dof.parser.Referencia;
import dof.parser.txt.TXTParser;
import dof.util.Util;

public class CIAF1800Parser extends TXTParser {

	private static final boolean ZERAR_SIT_AGUA_SETOR = false;

	Date dataGeracaoArquivo = null;

	private ArrayList<String> filtrarEscopos;

	private String header;

	Localidade localidade;

	Importer1800 importer;

	public CIAF1800Parser(Connection connection, String fileName, String[] args)
			throws SQLException, IOException {
		super(connection, fileName, args);

		this.localidade = Importer1800.localidadeFromFileName(connection, getSimpleFileName());
		// fileIsValid = false;
		rowStartOffset = -1;
		String importerType = Util.parseDefaultStringArg(args, "importer", "full");

		importer = Importer1800.createWithAuxiliar(importerType, this, connection);
	}

	@Override
	protected boolean shouldProcessThisFile() {
		if (filtrarEscopos.size() == 0)
			return true;

		// se tem escopos a filtrar, localidades não identificadas não podem ser
		// processadas
		if (this.localidade == null)
			return false;

		for (String escopo : filtrarEscopos) {
			if (this.localidade.pertence(escopo))
				return true;
		}

		return false;
	}

	@Override
	protected void parseArgs(String[] args) {
		filtrarEscopos = extractParams(args, "EMBASA", "DM", "DN", "DS", "UMS", "UMC", "UMF",
				"UML", "UMB", "UMJ", "UNA", "UNB", "UNF", "UNI", "UNE", "UNP", "UNS", "USC", "USI",
				"USU", "USJ", "USA", "USV");
	}

	private ArrayList<String> extractParams(String[] args, String... target) {
		ArrayList<String> r = new ArrayList<String>();
		for (String t : target) {
			if (Util.contains(args, t))
				r.add(t);
		}
		return r;
	}

	@Deprecated
	public static Integer extraiLocalidade_Old(String fileName) {
		Integer localidade;
		String s = fileName.replace("GEO", "");
		s = s.replace(".txt", "");
		s = s.replace(".errors", "");
		if (s.length() <= 4) {
			localidade = valorInt(s);
			return localidade;
		}
		if (s.length() == 5) {
			s = s.substring(0, 3);
			localidade = valorInt(s);
			return localidade;
		}
		return null;
	}

	public static Integer extraiLocalidade(String fileName) {
		if (fileName.startsWith("GEO"))
			return extraiLocalidade_Old(fileName);

		String s = fileName.substring(19, 23);
		Integer localidade = valorInt(s);
		return localidade;
	}

	@Override
	protected void processRow(int rowIndex, String content) throws IOException {
		super.processRow(rowIndex, content);

		startMonitoringAction("Parsing row");
		Row1800 row = null;
		try {

			row = parseLancRow(content, rowIndex);

		} finally {
			endMonitoringAction("Parsing row");
		}

		if (row == null) {
			importer.registrarInicioProcessamento(localidade, this.dataGeracaoArquivo);
			return;
		}
		
		importer.doImport(rowIndex, row);
		importer.registrarFimProcessamento(localidade, this.dataGeracaoArquivo);
	}

	@SuppressWarnings("unlikely-arg-type")
	protected Row1800 parseLancRow(String s, int rowIndex) {
		s = treatInvalidChars(s);
		if (rowIndex == 0) {

			if (s.startsWith("SC1800")) {
				try {
					parseHeader(s);
					fileIsValid = true;
					finished = false;

				} catch (Exception e) {
					System.out.println("Erro ao interpretar cabeçalho de data/hora de geração");
					e.printStackTrace();
					fileIsValid = false;
					finished = true;
				}

			} else {
				System.out.println("Arquivo faltando cabeçalho de data/hora de geração");
				finished = true;
				fileIsValid = false;
			}

			return null;

		} else
			checkRowLength(s);

		checkDataGeracao();

		Row1800 row = new Row1800();
		row.matricula = valorInt(s, 1, 9);
		row.nome = valorString(s, 10, 25);

		row.codLocalidade = valorInt(s, 105, 4);
		row.nome_localidade = null;
		row.setor = valorInt(s, 77, 2);
		row.quadra = valorInt(s, 79, 4);
		row.lote = valorInt(s, 83, 4);
		row.slote = valorInt(s, 87, 4);
		row.sslote = valorInt(s, 91, 4);
		row.lado = valorInt(s, 95, 4);
		row.cod_lograd = valorInt(s, 239, 6);
		row.tipo_lograd = valorString(s, 35, 3);
		row.titulo_lograd = valorString(s, 38, 5);
		row.endereco = valorString(s, 43, 30);
		row.porta = valorString(s, 130, 5);
		row.bairro = valorInt(s, 99, 6);

		row.tipo_resp = valorInt(s, 73, 1);
		row.grupo_fat = valorInt(s, 74, 3);
		row.sit_imovel = valorInt(s, 401, 1);
		row.indi_med = valorInt(s, 135, 1);
		row.sit_agua = valorInt(s, 136, 1);

		row.dt_lig_esgoto = valorDate(s, 137, 8);
		row.bac_esgoto = valorInt(s, 145, 4);
		row.sit_esgoto = valorInt(s, 149, 1);

		row.perc_esg = valorInt(s, 150, 2);

		int ano = valorInt(s, 152, 4);
		int mes = valorInt(s, 156, 2);
		if ((ano != 0) && (mes != 0))
			row.ref_atual = new Referencia(ano, mes);
		else
			row.ref_atual = null;

		row.agua = valorMonetario(s, 158, 13);
		row.esgoto = valorMonetario(s, 171, 13);
		row.servicos = valorMonetario(s, 184, 13);
		row.cod_anc = valorString(s, 428, 2);

		row.categ1 = valorCateg(s, 197, 5);
		row.categ2 = valorCateg(s, 202, 5);
		row.categ3 = valorCateg(s, 207, 5);
		row.categ4 = valorCateg(s, 212, 5);
		row.cons_med = valorInt(s, 217, 6);

		row.hidrometro = valorString(s, 245, 10);
		if (row.hidrometro.equals(""))
			row.hidrometro = null;
		row.dt_instal_hidr = valorDate(s, 255, 8);
		row.loc_hidr = valorInt(s, 263, 1);
		row.vazao_hidr = valorInt(s, 264, 2);
		row.marca_hidr = valorInt(s, 266, 2);
		row.tipo_hidr = valorInt(s, 270, 1);
		row.diametro_hidr = valorInt(s, 268, 2);

		row.rot_ent = valorInt(s, 231, 4);
		row.ord_ent = valorInt(s, 235, 4);
		row.rot_leit = valorInt(s, 271, 4);
		row.ord_leit = valorInt(s, 275, 4);
		row.leitura1 = valorLeitura(s, 279, 15);
		row.leitura2 = valorLeitura(s, 294, 15);
		row.leitura3 = valorLeitura(s, 309, 15);
		row.leitura4 = valorLeitura(s, 324, 15);
		row.leitura5 = valorLeitura(s, 339, 15);
		row.leitura6 = valorLeitura(s, 354, 15);
		row.meses_deb = ifNull(valorInt(s, 369, 3), 0);
		row.vl_tot_deb_hist = ifNull(valorMonetario(s, 372, 14), 0.0);
		row.vl_tot_deb_atu = ifNull(valorMonetario(s, 386, 14), 0.0);
		row.g_consumidor = valorString(s, 400, 1);
		row.codabast_altern = valorInt(s, 412, 2);
		row.setor_abast = valorInt(s, 410, 2);
		row.ref_ini_r = valorString(s, 430, 6);
		row.ref_fin_r = valorString(s, 436, 6);
		row.vl_financ_r = valorMonetario(s, 442, 13);
		row.vl_entrada = valorMonetario(s, 455, 13);
		row.qtdprest_r = valorInt(s, 468, 4);
		row.vlprest_r = valorMonetario(s, 472, 13);
		row.qtdpagas_r = valorInt(s, 485, 4);
		row.valpagas_r = valorMonetario(s, 489, 13);
		row.qtdresto_r = valorInt(s, 502, 4);
		row.vldebito_r = valorMonetario(s, 506, 13);
		row.dt_ligacao_agua = valorDate(s, 519, 8);
		row.dt_ult_relig = valorDate(s, 527, 8);
		row.dt_ult_corte = valorDate(s, 535, 8);
		row.dt_ult_supres = valorDate(s, 543, 8);
		row.rot_mcp = valorInt(s, 551, 4);
		row.seq_mcp = valorInt(s, 555, 9);
		row.ddd = valorInt(s, 564, 4);
		row.telefone = valorInt(s, 568, 9);
		row.cep = valorInt(s, 577, 9);
		row.tipo_pessoa = valorString(s, 586, 2);
		row.cpf_cnpj = valorLong(s, 588, 15);
		if (row.cpf_cnpj != null)
			if (row.cpf_cnpj.equals(0))
				row.cpf_cnpj = null;
//			else if (row.cpf_cnpj.equals(""))
//				row.cpf_cnpj = null;

		row.num_morad = valorInt(s, 603, 4);
		row.cod_clas_ppl = valorInt(s, 607, 2);
		row.cod_loc_ppl = valorInt(s, 609, 4);
		row.dig_cod_loc_ppl = valorInt(s, 613, 1);
		row.proc_jud = valorString(s, 614, 1);
		row.cod_resp = valorInt(s, 615, 6);
		row.matricula_principal = valorInt(s, 621, 9);
		if (row.matricula_principal == 0)
			row.matricula_principal = null;

		row.tipo_documento = valorString(s, 630, 6);
		row.num_documento = valorString(s, 636, 15);
		row.volume_faturado = valorInt(s, 651, 10);
		return row;
	}

	private void checkDataGeracao() {
		if (dataGeracaoArquivo == null)
			throw new RuntimeException("Data de geração do arquivo indefinida");
	}

	private String treatInvalidChars(String s) {
		return s;
	}

	private void checkRowLength(String s) {
		int tamPadrao = 660;
		int length = s.length();
		if (length != tamPadrao) {

			if (length == tamPadrao + 1) {
				if (s.endsWith(".")) {
					// Padrão antigo, que termina com "."
					return;
				}

				// throw new
				// RuntimeException("Tamanho da string diferente do previsto");
				// int posNome = 10;
				// int lengthNome = 26;
				// String antes = s.substring(0, posNome);
				// String depois = s.substring(lengthNome);
				// String nome = s.substring(posNome, posNome + lengthNome);
				// nome = nome.substring(0, nome.length() - 1);
				// System.out.println(antes + "--" + nome + "--" + depois);
				// throw new RuntimeException("Remover!");
				// return antes + nome + depois;
			}

			throw new RuntimeException("Linha com " + length + " caracteres (esperado " + tamPadrao
					+ ")");
		}
	}

	private void parseHeader(String header) {
		String s = header.replace("SC1800 - DATA E HORA DA EMISSAO : ", "");
		s = s.trim();
		String mask = "yyyyMMdd HHmmss";
		SimpleDateFormat df = new SimpleDateFormat(mask);

		try {

			dataGeracaoArquivo = df.parse(s);

		} catch (ParseException e) {
			throw new RuntimeException(e);
		}

		this.header = header;

	}

	protected Categoria valorCateg(String s, int pos, int tam) {
		s = extract(s, pos, tam);
		if (s.trim().isEmpty())
			return null;
		int cat = valorInt(s, 1, 2);
		int econ = valorInt(s, 3, 3);
		if (cat == 0) {
			if (econ == 0)
				return null;
			throw new RuntimeException("Categoria inválida");
		}
		if (econ == 0)
			econ = 1000;
		// TODO paliativo para contornar o problema da 1800 só trazer 3 casas
		// decimais para economias
		// Deu erro com um empreendimento com 1000 economias. (Mat. 073644331)

		Categoria r = new Categoria(cat, econ);
		return r;
	}

	protected Leitura valorLeitura(String s, int pos, int tam) {
		s = extract(s, pos, tam);
		if (s.trim().isEmpty())
			return null;
		int anl = valorInt(s, 1, 3);
		int leit = valorInt(s, 4, 6);
		int cons = valorInt(s, 10, 6);
		Leitura r = new Leitura(anl, leit, cons);
		return r;
	}

	protected Double valorMonetario(String s, int pos, int tam) {
		s = extract(s, pos, tam);
		return valorMonetario(s);
	}

	protected Long valorLong(String s, int ini, int tam) {
		String r = extract(s, ini, tam);
		return valorLong(r);
	}

	protected Date valorDate(String s, int ini, int tam, String mask) {
		String r = extract(s, ini, tam);
		if (r.equals("00000000"))
			return null;
		return valorDate(r, mask);
	}

	protected Date valorDate(String s, int ini, int tam) {
		return valorDate(s, ini, tam, "DDMMYYYY");
	}

	protected Date valorDate(String s, String mask) {
		SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyy");
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
	protected boolean rowContainsAllRequiredData(int rowId, String content) {
		return true;
	}

	@Override
	protected void writeAdditionalSavePointInfo(FileWriter w) {
		SimpleDateFormat sdf = new SimpleDateFormat();
		if (dataGeracaoArquivo == null)
			return;
		String s = sdf.format(dataGeracaoArquivo);
		try {

			w.write("dataHoraGeracaoArquivo=" + s + "\r\n");

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected void readSavePointAdditionalInfo(String line) {
		if (line.startsWith("dataHoraGeracaoArquivo")) {
			String[] ss = line.split("=");
			String s = ss[1].trim();
			SimpleDateFormat sdf = new SimpleDateFormat();
			try {

				dataGeracaoArquivo = sdf.parse(s);

			} catch (ParseException e) {
				throw new RuntimeException(e);
			}
		}
		// sobrescrever em classes inferiores
	}

	@Override
	protected String getFileNameToShow() {
		if (localidade != null)
			return localidade.ur + " - " + getSimpleFileName();
		else
			return "??? - " + getSimpleFileName();
	}

	public static Comparator<File> fileSortingComparatorFor1800(final Connection connection) {
		return new Comparator<File>() {

			@Override
			public int compare(File f1, File f2) {
				if (f1.equals(f2))
					return 0;
				if (f2.equals(f1))
					return 0;

				String name1 = f1.getName();
				String name2 = f2.getName();

				Localidade l1;
				Localidade l2;
				try {
					l1 = Importer1800.localidadeFromFileName(connection, name1);
				} catch (Exception e) {
					// arquivo fora do formato é ignorado - tanto faz
					l1 = null;
				}

				try {
					l2 = Importer1800.localidadeFromFileName(connection, name2);
				} catch (Exception e) {
					// arquivo fora do formato é ignorado - tanto faz
					l2 = null;
				}
				return comparePolos(l1, l2);
			}

			private int comparePolos(Localidade l1, Localidade l2) {
				int pp1 = prioridadeLocalidade(l1);
				int pp2 = prioridadeLocalidade(l2);
				return compareInt(pp1, pp2);
			}

			private int prioridadeLocalidade(Localidade l) {
				if (l == null)
					return 6;

				if ((l.localidade >= 900) && (l.localidade <= 903))
					return 1;
				if (l.localidade == 700)
					return 2;
				if (l.diretoria == 1) // primeiro DM
					return 3;
				if (l.diretoria == 3) // depois DS
					return 4;
				return 5; // DN por último
			}

			private int compareInt(int a, int b) {
				if (a > b)
					return 1;

				if (a < b)
					return -1;

				return 0;
			}

			// private ResultSet query(String sql) throws SQLException {
			// Statement st = connection.createStatement();
			// return st.executeQuery(sql);
			// }

		};
	}

	@Override
	protected void afterFinished() {
		// enfileira comando de processamento dos ganhos
		// String s = "select amf_processa_ganho_exec_servico(matricula) \r\n" +
		// "from view_amf_resumotrabalhadas1serv\r\n" +
		// "where data_execucao is not null \r\n" +
		// "  and mes_ref_consumo(matricula, data_execucao) is not null\r\n" +
		// "  and localidade(matricula) = " + localidade;
		// execSQL("insert into cmd_comandopendente values ('" + s + "');");
	}

	@Override
	protected void processAfterLastRow(boolean finished) {
		if (!ZERAR_SIT_AGUA_SETOR)
			return;
		else
			throw new RuntimeException("Operação ZERAR_SIT_AGUA_SETOR desativada!");

		// for (int setor : setoresAtualizados) {
		// System.err.println("Alterando situação das ligações antigas (L" +
		// codLocalidade + " S"
		// + setor + ")");
		// zeraSituacaoAguaUsuariosLocalSetor(codLocalidade, setor);
		// }
	}

	@Override
	protected String getHeader() {
		return header;
	}

}
