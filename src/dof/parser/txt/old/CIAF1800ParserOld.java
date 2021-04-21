package dof.parser.txt.old;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import dof.parser.Categoria;
import dof.parser.Leitura;
import dof.parser.Localidade;
import dof.parser.Referencia;
import dof.parser.txt.CIAF1800Parser;
import dof.parser.txt.TXTParser;

@Deprecated
public class CIAF1800ParserOld extends TXTParser {

	Localidade localidade;

	long matricula;
	String nome;
	int codLocalidade;
	String nome_localidade;
	int setor;
	int quadra;
	int lote;
	int slote;
	int sslote;
	int lado;
	int cod_lograd;
	String tipo_lograd;
	String titulo_lograd;
	String endereco;
	String porta;
	int bairro;
	int tipo_resp;
	int grupo_fat;
	int sit_imovel;
	int indi_med;
	int sit_agua;
	Date dt_lig_esgoto;
	int bac_esgoto;
	int sit_esgoto;
	int perc_esg;
	Referencia ref_atual;
	double agua;
	double esgoto;
	double servicos;
	Categoria categ1;
	Categoria categ2;
	Categoria categ3;
	Categoria categ4;
	int cons_med;
	String hidrometro;
	Date dt_instal_hidr;
	Integer loc_hidr;
	Integer vazao_hidr;
	Integer marca_hidr;
	Integer tipo_hidr;
	Integer diametro_hidr;
	int rot_ent;
	int ord_ent;
	int rot_leit;
	int ord_leit;
	Leitura leitura1;
	Leitura leitura2;
	Leitura leitura3;
	Leitura leitura4;
	Leitura leitura5;
	Leitura leitura6;
	int meses_deb;
	double vl_tot_deb_hist;
	double vl_tot_deb_atu;
	String g_consumidor;
	int codabast_altern;
	int setor_abast;
	String ref_ini_r;
	String ref_fin_r;
	double vl_financ_r;
	double vl_entrada;
	int qtdprest_r;
	double vlprest_r;
	int qtdpagas_r;
	double valpagas_r;
	int qtdresto_r;
	double vldebito_r;
	Date dt_ligacao_agua;
	Date dt_ult_relig;
	Date dt_ult_corte;
	Date dt_ult_supres;
	int rot_mcp;
	int seq_mcp;
	int ddd;
	int telefone;
	int cep;
	String tipo_pessoa;
	String cpf_cnpj;
	int num_morad;
	int cod_clas_ppl;
	int cod_loc_ppl;
	int dig_cod_loc_ppl;
	String proc_jud;
	int cod_resp;
	Long matricula_principal;
	String tipo_documento;
	String num_documento;
	int volume_faturado;
	private String cod_anc;

	private ArrayList<Integer> setores = new ArrayList<Integer>();

	private class ChaveLogradouro implements Comparable {
		int localidade;
		int codigo;

		public ChaveLogradouro(int localidade, int codigo) {
			super();
			this.localidade = localidade;
			this.codigo = codigo;
		}

		@Override
		public String toString() {
			return "ChaveLogradouro [localidade=" + localidade + ", codigo="
					+ codigo + "]";
		}

		@Override
		public boolean equals(Object obj) {
			ChaveLogradouro o = (ChaveLogradouro) obj;
			if (this.localidade != o.localidade)
				return false;
			if (this.codigo != o.codigo)
				return false;
			return true;
		}

		@Override
		public int compareTo(Object arg0) {
			// TODO Auto-generated method stub
			return 0;
		}

	}

	// private Boolean localidadeExistente;
	private List<ChaveLogradouro> logradourosAtualizados = new ArrayList<ChaveLogradouro>();
	private Date dataGeracaoArquivo = null;

	private boolean filtrarDiretoria;
	private boolean filtrarDM;
	private boolean filtrarDN;
	private boolean filtrarDS;

	public CIAF1800ParserOld(Connection connection, String fileName, String[] args) {
		super(connection, fileName, args);
		this.localidade = localidadeFromFileName(connection,
				getSimpleFileName());
		// fileIsValid = false;
		rowStartOffset = -1;
		// extraiLocalidade(getSimpleFileName());
	}

	@Override
	protected boolean shouldProcessThisFile() {
		if (!filtrarDiretoria)
			return true;
		Localidade loc = this.localidade;
		if (loc == null)
			return false;

		if (filtrarDM) {
			if (loc.diretoria == 1)
				return true;
		}

		if (filtrarDN) {
			if (loc.diretoria == 2)
				return true;
		}

		if (filtrarDS) {
			if (loc.diretoria == 3)
				return true;
		}

		return false;
	}

	@Override
	protected void parseArgs(String[] args) {
		if (containsAny(args, "DM", "DN", "DS")) {
			filtrarDiretoria = true;
			filtrarDM = false;
			filtrarDN = false;
			filtrarDS = false;
		}

		if (contains(args, "DM"))
			filtrarDM = true;
		if (contains(args, "DN"))
			filtrarDN = true;
		if (contains(args, "DS"))
			filtrarDS = true;

	}

	public static Localidade localidadeFromFileName(Connection connection,
			String fileName) {
		Integer localidade = CIAF1800Parser.extraiLocalidade(fileName);
		if (localidade == null)
			return null;

		String sql = "select u.diretoria as diretoria,\r\n"
				+ "       u.id as polo,\r\n"
				+ "       u.sigla as ur,\r\n"
				+ "       l.id as localidade\r\n"
				+ "from dof_localidade l join dof_unidaderegional u on l.unidade = u.id\r\n"
				+ "where l.id = " + localidade;
		Localidade l;
		try {
			ResultSet rs = query(connection, sql);
			if (!rs.next())
				return null;
			l = new Localidade();
			l.diretoria = rs.getInt("diretoria");
			l.polo = rs.getInt("polo");
			l.ur = rs.getString("ur");
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		l.localidade = localidade;
		return l;
	}

	protected void extraiLocalidadeFromFileName(String fileName) {
		codLocalidade = extraiLocalidade(fileName);
	}

	public static Integer extraiLocalidade(String fileName) {
		Integer localidade;
		String s = fileName.replace("GEO", "");
		s = s.replace(".txt", "");
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
		// throw new RuntimeException(
		// "Formato de nome de arquivo não reconhecido: " + fileName);
	}

	@Override
	protected void processRow(int rowIndex, String content) throws IOException {
		super.processRow(rowIndex, content);

		startMonitoringAction("Parsing row");
		boolean b = parseLancRow(content, rowIndex);
		endMonitoringAction("Parsing row");
		if (!b)
			return;

		if (rowIsAlreadyUpdated())
			return;

		atualizaSetor();
		atualizaLogradouro();

		startMonitoringAction("Importando usuario");
		atualizaUsuario();
		endMonitoringAction("Importando usuario");

		startMonitoringAction("Atualizando categoria");
		if (mudouCategoria())
			importaCategorias();
		endMonitoringAction("Atualizando categoria");

		if (hidrometro != null) {
			startMonitoringAction("Importando hidrometro");
			atualizaHidrometro();
			endMonitoringAction("Importando hidrometro");
			startMonitoringAction("Importando usuariohidrometro");
			atualizaUsuarioHidrometro();
			endMonitoringAction("Importando usuariohidrometro");
		}
		if (ref_atual != null) {
			startMonitoringAction("Importando fatura atual");
			atualizaFatura();
			endMonitoringAction("Importando fatura atual");

			startMonitoringAction("Importando Leituras");
			atualizaLeituras();
			endMonitoringAction("Importando Leituras");
		}
		startMonitoringAction("Importando cobrança");
		atualizaCobranca();
		endMonitoringAction("Importando cobrança");
		markMatriculaAsUpdated();
	}

	private void atualizaLogradouro() {
		startMonitoringAction("Importando logradouro");
		if (!logradouroAtualizado(codLocalidade, cod_lograd)) {
			importaLogradouro();
			// System.out.println("atualizando logradouro " + codLocalidade +
			// ", "
			// + cod_lograd);
			ChaveLogradouro c = new ChaveLogradouro(codLocalidade, cod_lograd);
			logradourosAtualizados.add(c);
		}
		endMonitoringAction("Importando logradouro");
	}

	private void atualizaSetor() throws IOException {
		startMonitoringAction("Importando logradouro");
		if (!setorAtualizado(codLocalidade, setor)) {
			if (!existeSetorNaBase(codLocalidade, setor)) {
				importaSetor();
				setores.add(setor);
			}
		}
		endMonitoringAction("Importando logradouro");
	}

	private boolean existeSetorNaBase(int codLocalidade, int setor) throws IOException {
		Long t;
		try {

			String sql = "select count(*) from dof_setor"
			+ " where localidade = %localidade and setor = %setor;";
			sql = assignParametersSetor(sql);
			t = (Long) singleValueQuery(sql);
			return t > 0;

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private void markMatriculaAsUpdated() throws IOException {
		String sql = "update dof_usuario set updated = "
				+ timestampFieldValue(dataGeracaoArquivo)
				+ " where matricula = " + matricula;
		execSQL(sql);
	}

	private boolean rowIsAlreadyUpdated() throws IOException {
		String sql = "select count(*) as total from dof_usuario where matricula = "
				+ matricula
				+ " and updated >= "
				+ timestampFieldValue(dataGeracaoArquivo);
		try {
			Long r = (Long) singleValueQuery(sql);
			if (r > 0)
				return true;

		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}

		return false;
	}

	private boolean mudouCategoria() throws IOException {
		ArrayList<Categoria> todo = getCategorias();

		try {
			ResultSet rs = query("select * from dof_usuariocategoria where matricula = "
					+ matricula + ";");

			while (rs.next()) {
				Integer cod = rs.getInt("categoria");
				Categoria c = findCategoria(todo, cod);
				if (c == null)
					return true;
				int eco = rs.getInt("economias");
				if (c.economias != eco)
					return true;
				todo.remove(c);
			}

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return todo.size() > 0;
	}

	private Categoria findCategoria(ArrayList<Categoria> todo,
			Integer codCategoria) {
		for (Categoria c : todo) {
			if (c.categoria == codCategoria)
				return c;
		}
		return null;
	}

	private ArrayList<Categoria> getCategorias() {
		ArrayList<Categoria> r = new ArrayList<Categoria>();
		if (categ1 != null)
			r.add(categ1);
		if (categ2 != null)
			r.add(categ2);
		if (categ3 != null)
			r.add(categ3);
		if (categ4 != null)
			r.add(categ4);
		return r;
	}

	private void importaLogradouro() {
		try {
			Statement s = connection.createStatement();

			String sql = "update dof_logradouro set " + "tipo = %tipo, "
					+ "titulo = %titulo, " + "nome = %nome "
					+ " where localidade = %localidade and codigo = %codigo;";
			sql = assignParametersLogradouro(sql);
			s.execute(sql);
			int u = s.getUpdateCount();
			if (u > 0)
				return;

			sql = "insert into dof_logradouro (localidade, codigo, tipo, titulo, nome) "
					+ "values (%localidade, %codigo, %tipo, %titulo, %nome);";
			sql = assignParametersLogradouro(sql);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private void importaSetor() {
		try {
			Statement s = connection.createStatement();

			String sql = "insert into dof_setor (localidade, setor) "
					+ "values (%localidade, %setor);";
			sql = assignParametersSetor(sql);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private String assignParametersSetor(String sql) {
		sql = sql.replace("%localidade", fieldValue(codLocalidade));
		sql = sql.replace("%setor", fieldValue(setor));
		return sql;
	}

	private String assignParametersLogradouro(String sql) {
		sql = sql.replace("%localidade", fieldValue(codLocalidade));
		sql = sql.replace("%codigo", fieldValue(cod_lograd));
		sql = sql.replace("%tipo", fieldValue(tipo_lograd));
		sql = sql.replace("%titulo", fieldValue(titulo_lograd));
		sql = sql.replace("%nome", fieldValue(endereco));
		return sql;
	}

	private boolean logradouroAtualizado(int localidade, int cod_lograd) {
		return logradourosAtualizados.contains(new ChaveLogradouro(localidade,
				cod_lograd));
	}

	private boolean setorAtualizado(int localidade, int setor) {
		return logradourosAtualizados.contains(setor);
	}

	protected void atualizaHidrometro() {
		// log("Atualizando hidrômetro de " + matricula);

		try {
			Statement s = connection.createStatement();

			String sql = "update dof_hidrometro set " + "vazao = %vazao, "
					+ "marca = %marca, " + "tipo = %tipo, "
					+ "diametro = %diametro"
					+ " where hidrometro = %hidrometro;";
			sql = assignParametersHidrometro(sql);
			s.execute(sql);
			int u = s.getUpdateCount();
			if (u > 0)
				return;

			sql = "insert into dof_hidrometro (hidrometro, vazao, marca, tipo, diametro) values (%hidrometro, %vazao, %marca, %tipo, %diametro);";
			sql = assignParametersHidrometro(sql);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	private String assignParametersHidrometro(String sql) {
		sql = sql.replace("%hidrometro", fieldValue(hidrometro));
		sql = sql.replace("%vazao", fieldValue(vazao_hidr));
		sql = sql.replace("%marca", fieldValue(marca_hidr));
		sql = sql.replace("%tipo", fieldValue(tipo_hidr));
		sql = sql.replace("%diametro", fieldValue(diametro_hidr));
		return sql;
	}

	private void atualizaFatura() throws IOException {
		// log("Atualizando fatura de " + matricula);

		try {
			Statement s = connection.createStatement();

			String sql = "update dof_fatura set "
					+ "agua = %agua, "
					+ "esgoto = %esgoto, "
					+ "servicos = %servicos, "
					+ "cod_anc = %cod_anc, "
					+ "volume_faturado = %volume_faturado, "
					+ "updatedfatura = %updf "
					+ " where matricula = %matricula and ano = %ano and mes = %mes "
					+ "and (updatedfatura is null or updatedfatura < "
					+ fieldValue(dataGeracaoArquivo) + ");";
			sql = assignParametersFatura(sql);
			s.execute(sql);
			int u = s.getUpdateCount();
			if (u > 0)
				return;

			sql = "insert into dof_fatura (matricula, ano, mes, agua, esgoto, servicos, cod_anc, volume_faturado, updatedfatura) "
					+ "values (%matricula, %ano, %mes, %agua, %esgoto, %servicos, %cod_anc, %volume_faturado, %updf);";
			sql = assignParametersFatura(sql);
			try {
				s.execute(sql);
			} catch (SQLException e) {
				sql = "select updatedfatura from dof_fatura where matricula = %matricula and ano = %ano and mes = %mes ";
				sql = assignParametersFatura(sql);
				Date d = (Date) singleValueQuery(sql);
				if (d != null)
					System.out
							.println("Importação para dof_fatura ignorada porque a fatura "
									+ ref_atual.mes
									+ "/"
									+ ref_atual.ano
									+ " na base está mais atualizada do que o arquivo "
									+ d);
				// solução para o bug dos casos onde ele tenta re-inserir os
				// registros de fatura por
				// conta de estarem mais desatualizados do que os que já estão
				// na base.
				else
					throw e;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	private String assignParametersFatura(String sql) {
		sql = sql.replace("%matricula", fieldValue(matricula));
		sql = sql.replace("%ano", fieldValue(ref_atual.ano));
		sql = sql.replace("%mes", fieldValue(ref_atual.mes));
		sql = sql.replace("%agua", fieldValue(agua));
		sql = sql.replace("%esgoto", fieldValue(esgoto));
		sql = sql.replace("%servicos", fieldValue(servicos));
		sql = sql.replace("%cod_anc", fieldValue(cod_anc));
		sql = sql.replace("%volume_faturado", fieldValue(volume_faturado));

		String t = timestampFieldValue(this.dataGeracaoArquivo);
		sql = sql.replace("%updl", t);
		sql = sql.replace("%updf", t);
		// sql = sql.replace("%updb", t);

		return sql;
	}

	protected void atualizaUsuarioHidrometro() {
		// log("Atualizando usuariohidrômetro de " + matricula);

		try {
			Statement s = connection.createStatement();

			String sql = "update dof_usuariohidrometro set "
					+ "dt_instal = %dt_instal "
					+ " where matricula = %matricula and hidrometro = %hidrometro;";
			sql = assignParametersUsuarioHidrometro(sql);
			s.execute(sql);
			int u = s.getUpdateCount();
			if (u > 0)
				return;

			sql = "insert into dof_usuariohidrometro (matricula, hidrometro, dt_instal) values (%matricula, %hidrometro, %dt_instal);";
			sql = assignParametersUsuarioHidrometro(sql);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	private String assignParametersUsuarioHidrometro(String sql) {
		sql = sql.replace("%matricula", fieldValue(matricula));
		sql = sql.replace("%hidrometro", fieldValue(hidrometro));
		sql = sql.replace("%dt_instal", fieldValue(dt_instal_hidr));
		return sql;
	}

	private void importaCategorias() throws IOException {
		// log("Atualizando categorias de " + matricula);
		arquivarUsuarioCategoria();

		String sql = "delete from dof_usuariocategoria where matricula = "
				+ matricula;
		execSQL(sql);

		if (categ1 != null)
			insertUsuarioCategoria(categ1);
		if (categ2 != null)
			insertUsuarioCategoria(categ2);
		if (categ3 != null)
			insertUsuarioCategoria(categ3);
		if (categ4 != null)
			insertUsuarioCategoria(categ4);
	}

	private void arquivarUsuarioCategoria() throws IOException {
		String sql = "select arquivar_usuariocategoria(" + matricula + ", "
				+ fieldValue(dataGeracaoArquivo) + ");";
		try {
			query(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private void arquivarEsgoto() throws IOException {
		String sql = "select arquivar_esgoto(" + matricula + ", "
				+ fieldValue(dataGeracaoArquivo) + ");";
		try {
			query(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private void log(String s) {
		System.out.println(s);
	}

	private void insertUsuarioCategoria(Categoria c) throws IOException {
		String sql = "insert into dof_usuariocategoria (matricula, categoria, economias) values ("
				+ matricula + ", " + c.categoria + ", " + c.economias + ");";
		execSQL(sql);
	}

	protected void atualizaUsuario() {
		// log("Atualizando usuário de " + matricula);

		try {
			Statement s = connection.createStatement();

			String sql = "update dof_usuario set " + "nome = %nome, "
					+ "localidade = %localidade, setor = %setor, "
					+ "quadra = %quadra, lote = %lote, slote = %slote, "
					+ "sslote = %sslote, lado = %lado, "
					+ "cod_lograd = %cod_lograd, "
					+ "bairro = %bairro, tipo_resp = %tipo_resp, "
					+ "grupo_fat = %grupo_fat, sit_imovel = %sit_imovel, "
					+ "indi_med = %indi_med, sit_agua = %sit_agua, "
					+ "dt_lig_esgoto = %dt_lig_esgoto, "
					+ "bac_esgoto = %bac_esgoto, sit_esgoto = %sit_esgoto, "
					+ "perc_esg = %perc_esg, cons_med = %cons_med, "
					+ "hidrometro = %hidrometro, loc_hidr = %loc_hidr, "
					+ "rot_ent = %rot_ent, ord_ent = %ord_ent, "
					+ "rot_leit = %rot_leit, ord_leit = %ord_leit, "
					+ "g_consumidor = %g_consumidor, "
					+ "codabast_altern = %codabast_altern, "
					+ "setor_abast = %setor_abast, "
					+ "dt_ligacao_agua = %dt_ligacao_agua, "
					+ "rot_mcp = %rot_mcp, seq_mcp = %seq_mcp, "
					+ "ddd = %ddd, telefone = %telefone, cep = %cep, "
					+ "tipo_pessoa = %tipo_pessoa, cpf_cnpj = %cpf_cnpj, "
					+ "num_morad = %num_morad, cod_resp = %cod_resp, "
					+ "matricula_principal = %matricula_principal, "
					+ "tipo_documento = %tipo_documento, "
					+ "num_documento = %num_documento "
					+ " where matricula = %matricula;";
			sql = assignParametersUsuario(sql);
			s.execute(sql);
			int u = s.getUpdateCount();
			if (u > 0)
				return;

			sql = "insert into dof_usuario (matricula,nome,localidade,setor,quadra,lote,slote,sslote,lado,cod_lograd,"
					+ "porta,bairro,tipo_resp,grupo_fat,sit_imovel,indi_med,sit_agua,dt_lig_esgoto,bac_esgoto,"
					+ "sit_esgoto,perc_esg,cons_med,hidrometro,loc_hidr,rot_ent,ord_ent,rot_leit,ord_leit,g_consumidor,"
					+ "codabast_altern,setor_abast,dt_ligacao_agua,rot_mcp,seq_mcp,ddd,telefone,cep,tipo_pessoa,cpf_cnpj,"
					+ "num_morad,cod_resp,matricula_principal,tipo_documento,num_documento)"
					+ " values (%matricula,%nome,%localidade,%setor,%quadra,%lote,%slote,%sslote,%lado,%cod_lograd,"
					+ "%porta,%bairro,%tipo_resp,%grupo_fat,%sit_imovel,%indi_med,%sit_agua,%dt_lig_esgoto,%bac_esgoto,"
					+ "%sit_esgoto,%perc_esg,%cons_med,%hidrometro,%loc_hidr,%rot_ent,%ord_ent,%rot_leit,%ord_leit,%g_consumidor,"
					+ "%codabast_altern,%setor_abast,%dt_ligacao_agua,%rot_mcp,%seq_mcp,%ddd,%telefone,%cep,%tipo_pessoa,%cpf_cnpj,"
					+ "%num_morad,%cod_resp,%matricula_principal,%tipo_documento,%num_documento);";
			sql = assignParametersUsuario(sql);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	private String assignParametersUsuario(String sql) {
		sql = sql.replace("%matricula_principal",
				fieldValue(matricula_principal));
		sql = sql.replace("%matricula", fieldValue(matricula));
		sql = sql.replace("%nome", fieldValue(nome));
		sql = sql.replace("%localidade", fieldValue(codLocalidade));
		sql = sql.replace("%setor_abast", fieldValue(setor_abast));
		sql = sql.replace("%setor", fieldValue(setor));
		sql = sql.replace("%quadra", fieldValue(quadra));
		sql = sql.replace("%sslote", fieldValue(sslote));
		sql = sql.replace("%slote", fieldValue(slote));
		sql = sql.replace("%lote", fieldValue(lote));
		sql = sql.replace("%lado", fieldValue(lado));
		sql = sql.replace("%cod_lograd", fieldValue(cod_lograd));
		// sql = sql.replace("%tipo_lograd", fieldValue(tipo_lograd));
		// sql = sql.replace("%titulo_lograd", fieldValue(titulo_lograd));
		// sql = sql.replace("%endereco", fieldValue(endereco));
		sql = sql.replace("%porta", fieldValue(porta));
		sql = sql.replace("%bairro", fieldValue(bairro));
		sql = sql.replace("%tipo_resp", fieldValue(tipo_resp));
		sql = sql.replace("%grupo_fat", fieldValue(grupo_fat));
		sql = sql.replace("%sit_imovel", fieldValue(sit_imovel));
		sql = sql.replace("%indi_med", fieldValue(indi_med));
		sql = sql.replace("%sit_agua", fieldValue(sit_agua));
		sql = sql.replace("%dt_lig_esgoto", fieldValue(dt_lig_esgoto));
		sql = sql.replace("%bac_esgoto", fieldValue(bac_esgoto));
		sql = sql.replace("%sit_esgoto", fieldValue(sit_esgoto));
		sql = sql.replace("%perc_esg", fieldValue(perc_esg));
		sql = sql.replace("%cons_med", fieldValue(cons_med));
		sql = sql.replace("%hidrometro", fieldValue(hidrometro));
		sql = sql.replace("%loc_hidr", fieldValue(loc_hidr));
		sql = sql.replace("%rot_ent", fieldValue(rot_ent));
		sql = sql.replace("%ord_ent", fieldValue(ord_ent));
		sql = sql.replace("%rot_leit", fieldValue(rot_leit));
		sql = sql.replace("%ord_leit", fieldValue(ord_leit));
		sql = sql.replace("%g_consumidor", fieldValue(g_consumidor));
		sql = sql.replace("%codabast_altern", fieldValue(codabast_altern));
		sql = sql.replace("%dt_ligacao_agua", fieldValue(dt_ligacao_agua));
		sql = sql.replace("%rot_mcp", fieldValue(rot_mcp));
		sql = sql.replace("%seq_mcp", fieldValue(seq_mcp));
		sql = sql.replace("%ddd", fieldValue(ddd));
		sql = sql.replace("%telefone", fieldValue(telefone));
		sql = sql.replace("%cep", fieldValue(cep));
		sql = sql.replace("%tipo_pessoa", fieldValue(tipo_pessoa));
		sql = sql.replace("%cpf_cnpj", fieldValue(cpf_cnpj));
		sql = sql.replace("%num_morad", fieldValue(num_morad));
		sql = sql.replace("%cod_resp", fieldValue(cod_resp));
		sql = sql.replace("%tipo_documento", fieldValue(tipo_documento));
		sql = sql.replace("%num_documento", fieldValue(num_documento));
		return sql;
	}

	private void atualizaLeituras() throws IOException {
		atualizaLeitura(leitura1, 0);
		atualizaLeitura(leitura2, -1);
		atualizaLeitura(leitura3, -2);
		atualizaLeitura(leitura4, -3);
		atualizaLeitura(leitura5, -4);
		atualizaLeitura(leitura6, -5);
	}

	private void atualizaLeitura(Leitura leitura, int shiftRef) throws IOException {
		Referencia ref = ref_atual.copy();
		ref.shift(shiftRef);
		atualizaLeitura(leitura, ref);
	}

	private void atualizaLeitura(Leitura leitura, Referencia ref) throws IOException {
		// log("Atualizando leitura de " + matricula);

		try {
			Statement s = connection.createStatement();

			String sql = "update dof_fatura set "
					+ "cod_anl = %cod_anl, "
					+ "leitura = %leitura, "
					+ "consumo = %consumo, "
					+ "updatedleitura = %updl "
					+ " where matricula = %matricula and ano = %ano and mes = %mes "
					+ "and (updatedleitura is null or updatedleitura < "
					+ fieldValue(dataGeracaoArquivo) + ");";

			// String sql = "update dof_fatura set "
			// + "cod_anl = %cod_anl, "
			// + "leitura = %leitura, "
			// + "consumo = %consumo "
			// +
			// " where matricula = %matricula and seq(ano, mes) = seq(%ano, %mes);";

			sql = assignParametersLeitura(sql, leitura, ref);
			s.execute(sql);
			int u = s.getUpdateCount();
			if (u > 0)
				return;

			if (leitura.cod_anl == 0)
				if (leitura.leitura == 0)
					if (leitura.consumo == 0)
						return;

			sql = "insert into dof_fatura (matricula, ano, mes, cod_anl, leitura, consumo, updatedleitura) "
					+ "values (%matricula, %ano, %mes, %cod_anl, %leitura, %consumo, %updl);";
			sql = assignParametersLeitura(sql, leitura, ref);
			try {
				s.execute(sql);
			} catch (SQLException e) {

				sql = "select updatedleitura from dof_fatura where matricula = %matricula and ano = %ano and mes = %mes ";
				sql = assignParametersLeitura(sql, leitura, ref);
				Date d = (Date) singleValueQuery(sql);
				if (d != null)
					System.out
							.println("Importação para dof_fatura ignorada porque a leitura "
									+ ref.mes
									+ "/"
									+ ref.ano
									+ " na base está mais atualizada do que o arquivo "
									+ d);

				// solução para o bug dos casos onde ele tenta re-inserir os
				// registros de fatura por
				// conta de estarem mais desatualizados do que os que já estão
				// na base.
				else
					throw e;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private String assignParametersLeitura(String sql, Leitura leit,
			Referencia ref) {
		sql = sql.replace("%matricula", fieldValue(matricula));
		sql = sql.replace("%ano", fieldValue(ref.ano));
		sql = sql.replace("%mes", fieldValue(ref.mes));
		sql = sql.replace("%cod_anl", fieldValue(leit.cod_anl));
		sql = sql.replace("%leitura", fieldValue(leit.leitura));
		sql = sql.replace("%consumo", fieldValue(leit.consumo));
		String t = timestampFieldValue(this.dataGeracaoArquivo);
		sql = sql.replace("%updl", t);
		// sql = sql.replace("%updf", t);
		// sql = sql.replace("%updb", t);
		return sql;
	}

	protected boolean parseLancRow(String s, long rowIndex) {
		if (rowIndex == 0) {
			if (s.startsWith("SC1800")) {
				parseHeader(s);
				fileIsValid = true;
				return false;
			} else
				fileIsValid = false;
		}

		if (!fileIsValid) {
			finished = true;
			return false;
		}
		matricula = valorLong(s, 1, 9);
		nome = valorString(s, 10, 25);

		codLocalidade = valorInt(s, 105, 4);
		nome_localidade = null;
		setor = valorInt(s, 77, 2);
		quadra = valorInt(s, 79, 4);
		lote = valorInt(s, 83, 4);
		slote = valorInt(s, 87, 4);
		sslote = valorInt(s, 91, 4);
		lado = valorInt(s, 95, 4);
		cod_lograd = valorInt(s, 239, 6);
		tipo_lograd = valorString(s, 35, 3);
		titulo_lograd = valorString(s, 38, 5);
		endereco = valorString(s, 43, 30);
		porta = valorString(s, 130, 5);
		bairro = valorInt(s, 99, 6);

		tipo_resp = valorInt(s, 73, 1);
		grupo_fat = valorInt(s, 74, 3);
		sit_imovel = valorInt(s, 401, 1);
		indi_med = valorInt(s, 135, 1);
		sit_agua = valorInt(s, 136, 1);

		dt_lig_esgoto = valorDate(s, 137, 8);
		bac_esgoto = valorInt(s, 145, 4);
		sit_esgoto = valorInt(s, 149, 1);

		perc_esg = valorInt(s, 150, 2);

		int ano = valorInt(s, 152, 4);
		int mes = valorInt(s, 156, 2);
		if ((ano != 0) && (mes != 0))
			ref_atual = new Referencia(ano, mes);
		else
			ref_atual = null;

		agua = valorMonetario(s, 158, 13);
		esgoto = valorMonetario(s, 171, 13);
		servicos = valorMonetario(s, 184, 13);
		cod_anc = valorString(s, 428, 2);

		categ1 = valorCateg(s, 197, 5);
		categ2 = valorCateg(s, 202, 5);
		categ3 = valorCateg(s, 207, 5);
		categ4 = valorCateg(s, 212, 5);
		cons_med = valorInt(s, 217, 6);

		hidrometro = valorString(s, 245, 10);
		if (hidrometro.equals(""))
			hidrometro = null;
		dt_instal_hidr = valorDate(s, 255, 8);
		loc_hidr = valorInt(s, 263, 1);
		vazao_hidr = valorInt(s, 264, 2);
		marca_hidr = valorInt(s, 266, 2);
		tipo_hidr = valorInt(s, 270, 1);
		diametro_hidr = valorInt(s, 268, 2);

		rot_ent = valorInt(s, 231, 4);
		ord_ent = valorInt(s, 235, 4);
		rot_leit = valorInt(s, 271, 4);
		ord_leit = valorInt(s, 275, 4);
		leitura1 = valorLeitura(s, 279, 15);
		leitura2 = valorLeitura(s, 294, 15);
		leitura3 = valorLeitura(s, 309, 15);
		leitura4 = valorLeitura(s, 324, 15);
		leitura5 = valorLeitura(s, 339, 15);
		leitura6 = valorLeitura(s, 354, 15);
		meses_deb = valorInt(s, 369, 3);
		vl_tot_deb_hist = valorMonetario(s, 372, 14);
		vl_tot_deb_atu = valorMonetario(s, 386, 14);
		g_consumidor = valorString(s, 400, 1);
		codabast_altern = valorInt(s, 412, 2);
		setor_abast = valorInt(s, 410, 2);
		ref_ini_r = valorString(s, 430, 6);
		ref_fin_r = valorString(s, 436, 6);
		vl_financ_r = valorMonetario(s, 442, 13);
		vl_entrada = valorMonetario(s, 455, 13);
		qtdprest_r = valorInt(s, 468, 4);
		vlprest_r = valorMonetario(s, 472, 13);
		qtdpagas_r = valorInt(s, 485, 4);
		valpagas_r = valorMonetario(s, 489, 13);
		qtdresto_r = valorInt(s, 502, 4);
		vldebito_r = valorMonetario(s, 506, 13);
		dt_ligacao_agua = valorDate(s, 519, 8);
		dt_ult_relig = valorDate(s, 527, 8);
		dt_ult_corte = valorDate(s, 535, 8);
		dt_ult_supres = valorDate(s, 543, 8);
		rot_mcp = valorInt(s, 551, 4);
		seq_mcp = valorInt(s, 555, 9);
		ddd = valorInt(s, 564, 4);
		telefone = valorInt(s, 568, 9);
		cep = valorInt(s, 577, 9);
		tipo_pessoa = valorString(s, 586, 2);
		cpf_cnpj = valorString(s, 588, 15);
		if (cpf_cnpj.equals("0"))
			cpf_cnpj = null;
		else if (cpf_cnpj.equals(""))
			cpf_cnpj = null;

		num_morad = valorInt(s, 603, 4);
		cod_clas_ppl = valorInt(s, 607, 2);
		cod_loc_ppl = valorInt(s, 609, 4);
		dig_cod_loc_ppl = valorInt(s, 613, 1);
		proc_jud = valorString(s, 614, 1);
		cod_resp = valorInt(s, 615, 6);
		matricula_principal = valorLong(s, 621, 9);
		if (matricula_principal == 0)
			matricula_principal = null;

		tipo_documento = valorString(s, 630, 6);
		num_documento = valorString(s, 636, 15);
		volume_faturado = valorInt(s, 651, 10);
		return true;
	}

	private void parseHeader(String header) {
		String s = header.replace("SC1800 - DATA E HORA DA EMISSAO : ", "");
		String mask = "yyyyMMdd HHmmss";
		SimpleDateFormat df = new SimpleDateFormat(mask);

		try {

			dataGeracaoArquivo = df.parse(s);

		} catch (ParseException e) {
			throw new RuntimeException(e);
		}

	}

	protected Categoria valorCateg(String s, int pos, int tam) {
		s = extract(s, pos, tam);
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

	protected long valorLong(String s, int ini, int tam) {
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

	private void atualizaCobranca() {
		// log("Atualizando cobrança de " + matricula);

		try {
			Statement s = connection.createStatement();

			String sql = "update dof_cobranca set meses_deb = %meses_deb, "
					+ "vl_tot_deb_hist = %vl_tot_deb_hist, "
					+ "vl_tot_deb_atu = %vl_tot_deb_atu, "
					+ "ref_ini_r = %ref_ini_r, " + "ref_fin_r = %ref_fin_r, "
					+ "vl_financ_r = %vl_financ_r, "
					+ "vl_entrada = %vl_entrada, "
					+ "qtdprest_r = %qtdprest_r, " + "vlprest_r = %vlprest_r, "
					+ "qtdpagas_r = %qtdpagas_r, "
					+ "valpagas_r = %valpagas_r, "
					+ "qtdresto_r = %qtdresto_r, "
					+ "vldebito_r = %vldebito_r, "
					+ "dt_ult_relig = %dt_ult_relig, "
					+ "dt_ult_corte = %dt_ult_corte, "
					+ "dt_ult_supres = %dt_ult_supres, "
					+ "proc_jud = %proc_jud where matricula = %matricula;";
			sql = assignParametersCobranca(sql);
			s.execute(sql);
			int u = s.getUpdateCount();
			if (u > 0)
				return;

			sql = "insert into dof_cobranca (matricula, meses_deb, vl_tot_deb_hist, vl_tot_deb_atu, ref_ini_r, ref_fin_r, vl_financ_r, vl_entrada, qtdprest_r, vlprest_r, qtdpagas_r, valpagas_r, qtdresto_r, vldebito_r, dt_ult_relig, dt_ult_corte, dt_ult_supres, proc_jud) "
					+ "values (%matricula, %meses_deb, %vl_tot_deb_hist, %vl_tot_deb_atu, %ref_ini_r, %ref_fin_r, %vl_financ_r, %vl_entrada, %qtdprest_r, %vlprest_r, %qtdpagas_r, %valpagas_r, %qtdresto_r, %vldebito_r, %dt_ult_relig, %dt_ult_corte, %dt_ult_supres, %proc_jud);";
			sql = assignParametersCobranca(sql);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	private String assignParametersCobranca(String sql) {
		sql = sql.replace("%matricula", fieldValue(matricula));
		sql = sql.replace("%meses_deb", fieldValue(meses_deb));
		sql = sql.replace("%vl_tot_deb_hist", fieldValue(vl_tot_deb_hist));
		sql = sql.replace("%vl_tot_deb_atu", fieldValue(vl_tot_deb_atu));
		sql = sql.replace("%ref_ini_r", fieldValue(ref_ini_r));
		sql = sql.replace("%ref_fin_r", fieldValue(ref_fin_r));
		sql = sql.replace("%vl_financ_r", fieldValue(vl_financ_r));
		sql = sql.replace("%vl_entrada", fieldValue(vl_entrada));
		sql = sql.replace("%qtdprest_r", fieldValue(qtdprest_r));
		sql = sql.replace("%vlprest_r", fieldValue(vlprest_r));
		sql = sql.replace("%qtdpagas_r", fieldValue(qtdpagas_r));
		sql = sql.replace("%valpagas_r", fieldValue(valpagas_r));
		sql = sql.replace("%qtdresto_r", fieldValue(qtdresto_r));
		sql = sql.replace("%vldebito_r", fieldValue(vldebito_r));
		sql = sql.replace("%dt_ult_relig", fieldValue(dt_ult_relig));
		sql = sql.replace("%dt_ult_corte", fieldValue(dt_ult_corte));
		sql = sql.replace("%dt_ult_supres", fieldValue(dt_ult_supres));
		sql = sql.replace("%proc_jud", fieldValue(proc_jud));
		return sql;
	}

	@Override
	protected boolean rowContainsAllRequiredData(long rowId, String content) {
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

	public static Comparator<File> fileSortingComparatorFor1800(
			final Connection connection) {
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
					l1 = localidadeFromFileName(connection, name1);
				} catch (Exception e) {
					// arquivo fora do formato é ignorado - tanto faz
					l1 = null;
				}

				try {
					l2 = localidadeFromFileName(connection, name2);
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

}
