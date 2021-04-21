package dof.parser.txt.F1800;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;

import dof.parser.Categoria;
import dof.parser.Leitura;
import dof.parser.Localidade;
import dof.parser.Parser;
import dof.parser.Referencia;
import dof.parser.txt.F1800.tmp.Importer1800ConsMedInicialFatura;
import dof.parser.txt.F1800.tmp.Importer1800HistoricoInicialTitular;
import dof.util.SQL;

public abstract class Importer1800 {

	CIAF1800Parser parser;
	Connection connection;
	Importer1800 auxImporter;

	public Importer1800(CIAF1800Parser parser, Connection connection) {
		super();
		this.parser = parser;
		this.connection = connection;
	}

	public static Importer1800 create(String type, CIAF1800Parser parser, Connection connection) {
		if (type.equalsIgnoreCase("full"))
			return new FullImporter1800(parser, connection);

		// Importadores temporários - TODO remover após povoar
		if (type.equalsIgnoreCase("HistoricoInicialTitular"))
			return new Importer1800HistoricoInicialTitular(parser, connection);
		if (type.equalsIgnoreCase("ConsMedInicialFatura"))
			return new Importer1800ConsMedInicialFatura(parser, connection);

		throw new RuntimeException("Tipo de Importer1800 desconhecido: " + type);
	}

	public static Importer1800 createWithAuxiliar(String type, CIAF1800Parser parser,
			Connection connection) {
		String[] ts = type.split(",", 2);
		Importer1800 r = create(ts[0], parser, connection);
		if (ts.length == 2) {
			Importer1800 aux = createWithAuxiliar(ts[1], parser, connection);
			r.auxImporter = aux;
		}
		return r;
	}

	public final void doImport(int rowIndex, Row1800 row) throws IOException {
		internalImport(rowIndex, row);
		if (auxImporter != null)
			auxImporter.doImport(rowIndex, row);
	}

	protected abstract void internalImport(int rowIndex, Row1800 row) throws IOException;

	public static Localidade localidadeFromFileName(Connection connection, String fileName)
			throws IOException {
		Integer localidade = CIAF1800Parser.extraiLocalidade(fileName);
		if (localidade == null)
			return null;

		String sql = "select u.regiao as diretoria, u.id as polo,\r\n"
				+ "       u.sigla as ur, l.id as localidade\r\n"
				+ "from dof_localidade l join dof_unidaderegional u on l.unidade = u.id\r\n"
				+ "where l.id = " + localidade;
		Localidade l;
		try {
			ResultSet rs = SQL.query(connection, sql);
			if (!rs.next()) {
				//return null;
//				/*criar um nova localidade, 
//				mas se ele não exitir, pois ele já pode existir sem und_regional
//				ai a consulta anterior não pega. */
//				sql = "select id from dof_localidade where id = "+ localidade;
//				rs = SQL.query(connection, sql);
//				if (!rs.next()) {
					sql = "insert into dof_localidade(id, nome) values ("+ localidade +", '?' ) on conflict do nothing";
					Statement s = connection.createStatement();
					s.execute(sql);
					l = new Localidade();
					l.localidade = localidade;
//				} else {
//					l = null;
//				}
			} else {
				l = new Localidade();
				l.diretoria = rs.getInt("diretoria");
				l.polo = rs.getInt("polo");
				l.ur = rs.getString("ur");
				l.localidade = rs.getInt("localidade");
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		//l.localidade = localidade;
		return l;
	}

	protected String assignParametersFatura(String sql, Row1800 row) {
		sql = sql.replace("%matricula", SQL.fieldValue(row.matricula));
		sql = sql.replace("%ano", SQL.fieldValue(row.ref_atual.ano));
		sql = sql.replace("%mes", SQL.fieldValue(row.ref_atual.mes));
		sql = sql.replace("%agua", SQL.fieldValue(row.agua));
		sql = sql.replace("%esgoto", SQL.fieldValue(row.esgoto));
		sql = sql.replace("%servicos", SQL.fieldValue(row.servicos));
		sql = sql.replace("%cod_anc", SQL.fieldValue(row.cod_anc));
		sql = sql.replace("%cons_med", SQL.fieldValue(row.cons_med));
		sql = sql.replace("%volume_faturado", SQL.fieldValue(row.volume_faturado));
		sql = sql.replace("%force", SQL.fieldValue(Parser.FORCE_UPDATE));

		String t = SQL.timestampFieldValue(parser.dataGeracaoArquivo);
		sql = sql.replace("%updl", t);
		sql = sql.replace("%updf", t);
		// sql = sql.replace("%updb", t);

		return sql;
	}

	protected String assignParametersLeitura(String sql, int matricula, Leitura leit, Referencia ref) {
		sql = sql.replace("%matricula", SQL.fieldValue(matricula));
		sql = sql.replace("%ano", SQL.fieldValue(ref.ano));
		sql = sql.replace("%mes", SQL.fieldValue(ref.mes));
		sql = sql.replace("%cod_anl", SQL.fieldValue(leit.cod_anl));
		sql = sql.replace("%leitura", SQL.fieldValue(leit.leitura));
		sql = sql.replace("%consumo", SQL.fieldValue(leit.consumo));
		String t = SQL.timestampFieldValue(parser.dataGeracaoArquivo);
		sql = sql.replace("%updl", t);
		sql = sql.replace("%force", SQL.fieldValue(Parser.FORCE_UPDATE));
		// sql = sql.replace("%updf", t);
		// sql = sql.replace("%updb", t);
		return sql;
	}

	protected boolean mudouCategoria(Row1800 row) throws IOException {
		ArrayList<Categoria> todo = row.getCategorias();

		try {
			ResultSet rs = parser.query("select * from dof_usuariocategoria where matricula = "
					+ row.matricula + ";");

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

	protected void zeraSituacaoAguaUsuariosLocalSetor(int localidade, int setor, Row1800 row) {
		try {
			Statement s = connection.createStatement();

			String sql = "update dof_usuario set sit_agua = 0 "
					+ "where localidade = %localidade and setor = %setor "
					+ "  and not matricula in (%materros)" + "  and updated < %datager;";
			sql = assignParametersSetor(sql, localidade, setor, row.grupo_fat);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	protected boolean existeSetorNaBase(int codLocalidade, int setor) throws IOException {
		Integer t;
		try {

			String sql = "select count(*)::int from dof_setor"
					+ " where localidade = %localidade and setor = %setor;";
			sql = assignParametersSetor(sql, codLocalidade, setor);
			t = (Integer) parser.singleValueQuery(sql);
			return t > 0;

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	protected void markMatriculaAsUpdated(int matricula) throws IOException {
		String sql = "update dof_usuario set updated = "
				+ SQL.timestampFieldValue(parser.dataGeracaoArquivo) + " where matricula = "
				+ matricula;
		parser.execSQL(sql);
	}

	protected boolean matriculaIsAlreadyUpdated(int matricula) throws IOException {
		String sql = "select count(*)::int as total from dof_usuario where matricula = " + matricula
				+ " and updated >= " + SQL.timestampFieldValue(parser.dataGeracaoArquivo);
		try {
			Integer r = (Integer) parser.singleValueQuery(sql);
			if (r > 0)
				return true;

		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}

		return false;
	}

	protected void atualizaCobranca(Row1800 row) {

		try {
			Statement s = connection.createStatement();

			String sql = "update dof_cobranca set meses_deb = %meses_deb, "
					+ "vl_tot_deb_hist = %vl_tot_deb_hist, " + "vl_tot_deb_atu = %vl_tot_deb_atu, "
					+ "ref_ini_r = %ref_ini_r, " + "ref_fin_r = %ref_fin_r, "
					+ "vl_financ_r = %vl_financ_r, " + "vl_entrada = %vl_entrada, "
					+ "qtdprest_r = %qtdprest_r, " + "vlprest_r = %vlprest_r, "
					+ "qtdpagas_r = %qtdpagas_r, " + "valpagas_r = %valpagas_r, "
					+ "qtdresto_r = %qtdresto_r, " + "vldebito_r = %vldebito_r, "
					+ "dt_ult_relig = %dt_ult_relig, " + "dt_ult_corte = %dt_ult_corte, "
					+ "dt_ult_supres = %dt_ult_supres, "
					+ "proc_jud = %proc_jud where matricula = %matricula;";
			sql = assignParametersCobranca(sql, row);
			s.execute(sql);
			int u = s.getUpdateCount();
			if (u > 0)
				return;

			sql = "insert into dof_cobranca (matricula, meses_deb, vl_tot_deb_hist, vl_tot_deb_atu, ref_ini_r, ref_fin_r, vl_financ_r, vl_entrada, qtdprest_r, vlprest_r, qtdpagas_r, valpagas_r, qtdresto_r, vldebito_r, dt_ult_relig, dt_ult_corte, dt_ult_supres, proc_jud) "
					+ "values (%matricula, %meses_deb, %vl_tot_deb_hist, %vl_tot_deb_atu, %ref_ini_r, %ref_fin_r, %vl_financ_r, %vl_entrada, %qtdprest_r, %vlprest_r, %qtdpagas_r, %valpagas_r, %qtdresto_r, %vldebito_r, %dt_ult_relig, %dt_ult_corte, %dt_ult_supres, %proc_jud);";
			sql = assignParametersCobranca(sql, row);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	protected String assignParametersCobranca(String sql, Row1800 row) {
		sql = sql.replace("%matricula", SQL.fieldValue(row.matricula));
		sql = sql.replace("%meses_deb", SQL.fieldValue(row.meses_deb));
		sql = sql.replace("%vl_tot_deb_hist", SQL.fieldValue(row.vl_tot_deb_hist));
		sql = sql.replace("%vl_tot_deb_atu", SQL.fieldValue(row.vl_tot_deb_atu));
		sql = sql.replace("%ref_ini_r", SQL.fieldValue(row.ref_ini_r));
		sql = sql.replace("%ref_fin_r", SQL.fieldValue(row.ref_fin_r));
		sql = sql.replace("%vl_financ_r", SQL.fieldValue(row.vl_financ_r));
		sql = sql.replace("%vl_entrada", SQL.fieldValue(row.vl_entrada));
		sql = sql.replace("%qtdprest_r", SQL.fieldValue(row.qtdprest_r));
		sql = sql.replace("%vlprest_r", SQL.fieldValue(row.vlprest_r));
		sql = sql.replace("%qtdpagas_r", SQL.fieldValue(row.qtdpagas_r));
		sql = sql.replace("%valpagas_r", SQL.fieldValue(row.valpagas_r));
		sql = sql.replace("%qtdresto_r", SQL.fieldValue(row.qtdresto_r));
		sql = sql.replace("%vldebito_r", SQL.fieldValue(row.vldebito_r));
		sql = sql.replace("%dt_ult_relig", SQL.fieldValue(row.dt_ult_relig));
		sql = sql.replace("%dt_ult_corte", SQL.fieldValue(row.dt_ult_corte));
		sql = sql.replace("%dt_ult_supres", SQL.fieldValue(row.dt_ult_supres));
		sql = sql.replace("%proc_jud", SQL.fieldValue(row.proc_jud));
		return sql;
	}

	protected void atualizaHidrometro(Row1800 row) {
		// log("Atualizando hidrômetro de " + matricula);

		try {
			Statement s = connection.createStatement();

			String sql = "update dof_hidrometro set " + "vazao = %vazao, " + "marca = %marca, "
					+ "tipo = %tipo, " + "diametro = %diametro"
					+ " where hidrometro = %hidrometro;";
			sql = assignParametersHidrometro(sql, row);
			s.execute(sql);
			int u = s.getUpdateCount();
			if (u > 0)
				return;

			sql = "insert into dof_hidrometro (hidrometro, vazao, marca, tipo, diametro) values (%hidrometro, %vazao, %marca, %tipo, %diametro);";
			sql = assignParametersHidrometro(sql, row);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	protected String assignParametersHidrometro(String sql, Row1800 row) {
		sql = sql.replace("%hidrometro", SQL.fieldValue(row.hidrometro));
		sql = sql.replace("%vazao", SQL.fieldValue(row.vazao_hidr));
		sql = sql.replace("%marca", SQL.fieldValue(row.marca_hidr));
		sql = sql.replace("%tipo", SQL.fieldValue(row.tipo_hidr));
		sql = sql.replace("%diametro", SQL.fieldValue(row.diametro_hidr));
		return sql;
	}

	protected void atualizaFatura(Row1800 row) throws IOException {
		String sql = "select upsert_dof_fatura_fatura(%matricula, %ano, %mes, %agua, "
				+ "%esgoto, %servicos, %cod_anc, %cons_med, %volume_faturado, %updf, %force);";

		sql = assignParametersFatura(sql, row);
		try {

			parser.singleValueQuery(sql);

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	protected void atualizaUsuarioHidrometro(Row1800 row) {
		// log("Atualizando usuariohidrômetro de " + matricula);

		try {
			Statement s = connection.createStatement();

			String sql = "update dof_usuariohidrometro set " + "dt_instal = %dt_instal "
					+ " where matricula = %matricula and hidrometro = %hidrometro;";
			sql = assignParametersUsuarioHidrometro(sql, row);
			s.execute(sql);
			int u = s.getUpdateCount();
			if (u > 0)
				return;

			sql = "insert into dof_usuariohidrometro (matricula, hidrometro, dt_instal) values (%matricula, %hidrometro, %dt_instal);";
			sql = assignParametersUsuarioHidrometro(sql, row);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	protected String assignParametersUsuarioHidrometro(String sql, Row1800 row) {
		sql = sql.replace("%matricula", SQL.fieldValue(row.matricula));
		sql = sql.replace("%hidrometro", SQL.fieldValue(row.hidrometro));
		sql = sql.replace("%dt_instal", SQL.fieldValue(row.dt_instal_hidr));
		return sql;
	}

	protected void importaCategorias(Row1800 row) throws IOException {
		// log("Atualizando categorias de " + matricula);
		arquivarUsuarioCategoria(row);

		String sql = "delete from dof_usuariocategoria where matricula = " + row.matricula;
		parser.execSQL(sql);

		if (row.categ1 != null)
			insertUsuarioCategoria(row.categ1, row.matricula);
		if (row.categ2 != null)
			insertUsuarioCategoria(row.categ2, row.matricula);
		if (row.categ3 != null)
			insertUsuarioCategoria(row.categ3, row.matricula);
		if (row.categ4 != null)
			insertUsuarioCategoria(row.categ4, row.matricula);
	}

	protected void arquivarUsuarioCategoria(Row1800 row) throws IOException {
		String sql = "select arquivar_usuariocategoria(" + row.matricula + ", "
				+ SQL.fieldValue(parser.dataGeracaoArquivo) + ");";
		try {
			parser.query(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	protected void arquivarAguaSeMudou(Row1800 row) throws IOException {
		String sql = "select arquivar_agua(matricula, %datager) from dof_usuario "
				+ " where matricula = %mat "
				+ "   and (sit_agua <> %sit_agua or codabast_altern <> %abast_altern);";
		sql = sql.replace("%mat", SQL.fieldValue(row.matricula));
		sql = sql.replace("%datager", SQL.fieldValue(parser.dataGeracaoArquivo));
		sql = sql.replace("%abast_altern", SQL.fieldValue(row.codabast_altern));
		sql = sql.replace("%sit_agua", SQL.fieldValue(row.sit_agua));

		try {
			parser.query(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	protected void arquivarTitularSeMudou(Row1800 row) throws IOException {
		String sql = "select arquivar_titular(matricula, %datager, cpf_cnpj, nome) "
				+ "from dof_usuario where matricula = %mat " + 
				"   and (((cpf_cnpj is null) <> (%cpf_cnpj is null))"
				+ " or (cpf_cnpj <> %cpf_cnpj) or (nome <> %nome));";
		sql = sql.replace("%mat", SQL.fieldValue(row.matricula));
		sql = sql.replace("%datager", SQL.fieldValue(parser.dataGeracaoArquivo));
		sql = sql.replace("%cpf_cnpj", SQL.fieldValue(row.cpf_cnpj));
		sql = sql.replace("%nome", SQL.fieldValue(row.nome));

		try {
			parser.query(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	protected void arquivarTitular(int matricula, Long cpf, String nome) throws IOException {
		String sql = "select arquivar_titular(%mat, %datager, %cpf_cnpj, %nome);";
		sql = sql.replace("%mat", SQL.fieldValue(matricula));
		sql = sql.replace("%datager", SQL.fieldValue(parser.dataGeracaoArquivo));
		sql = sql.replace("%cpf_cnpj", SQL.fieldValue(cpf));
		sql = sql.replace("%nome", SQL.fieldValue(nome));

		try {
			parser.query(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	void log(String s) {
		System.out.println(s);
	}

	protected void insertUsuarioCategoria(Categoria c, int matricula) throws IOException {
		String sql = "insert into dof_usuariocategoria (matricula, categoria, economias) values ("
				+ matricula + ", " + c.categoria + ", " + c.economias + ");";
		parser.execSQL(sql);
	}

	protected void atualizaUsuario(Row1800 row) {
		// log("Atualizando usuário de " + matricula);

		try {
			Statement s = connection.createStatement();

			String sql = "update dof_usuario set nome = %nome, "
					+ "localidade = %localidade, setor = %setor, "
					+ "quadra = %quadra, lote = %lote, slote = %slote, "
					+ "sslote = %sslote, lado = %lado, cod_lograd = %cod_lograd, "
					+ "bairro = %bairro, tipo_resp = %tipo_resp, "
					+ "grupo_fat = %grupo_fat, sit_imovel = %sit_imovel, "
					+ "indi_med = %indi_med, sit_agua = %sit_agua, "
					+ "dt_lig_esgoto = %dt_lig_esgoto, "
					+ "bac_esgoto = %bac_esgoto, sit_esgoto = %sit_esgoto, "
					+ "perc_esg = %perc_esg, cons_med = %cons_med, "
					+ "hidrometro = %hidrometro, loc_hidr = %loc_hidr, "
					+ "rot_ent = %rot_ent, ord_ent = %ord_ent, "
					+ "rot_leit = %rot_leit, ord_leit = %ord_leit, "
					+ "g_consumidor = %g_consumidor, codabast_altern = %codabast_altern, "
					+ "setor_abast = %setor_abast, dt_ligacao_agua = %dt_ligacao_agua, "
					+ "rot_mcp = %rot_mcp, seq_mcp = %seq_mcp, "
					+ "ddd = %ddd, telefone = %telefone, cep = %cep, "
					+ "tipo_pessoa = %tipo_pessoa, cpf_cnpj = %cpf_cnpj, "
					+ "num_morad = %num_morad, cod_resp = %cod_resp, "
					+ "matricula_principal = %matricula_principal, "
					+ "tipo_documento = %tipo_documento, num_documento = %num_documento "
					+ " where matricula = %matricula;";
			sql = assignParametersUsuario(sql, row);
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
			sql = assignParametersUsuario(sql, row);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	protected String assignParametersUsuario(String sql, Row1800 row) {
		sql = sql.replace("%matricula_principal", SQL.fieldValue(row.matricula_principal));
		sql = sql.replace("%matricula", SQL.fieldValue(row.matricula));
		sql = sql.replace("%nome", SQL.fieldValue(row.nome));
		sql = sql.replace("%localidade", SQL.fieldValue(row.codLocalidade));
		sql = sql.replace("%setor_abast", SQL.fieldValue(row.setor_abast));
		sql = sql.replace("%setor", SQL.fieldValue(row.setor));
		sql = sql.replace("%quadra", SQL.fieldValue(row.quadra));
		sql = sql.replace("%sslote", SQL.fieldValue(row.sslote));
		sql = sql.replace("%slote", SQL.fieldValue(row.slote));
		sql = sql.replace("%lote", SQL.fieldValue(row.lote));
		sql = sql.replace("%lado", SQL.fieldValue(row.lado));
		sql = sql.replace("%cod_lograd", SQL.fieldValue(row.cod_lograd));
		// sql = sql.replace("%tipo_lograd", SQLUtil.fieldValue(tipo_lograd));
		// sql = sql.replace("%titulo_lograd",
		// SQLUtil.fieldValue(titulo_lograd));
		// sql = sql.replace("%endereco", SQLUtil.fieldValue(endereco));
		sql = sql.replace("%porta", SQL.fieldValue(row.porta));
		sql = sql.replace("%bairro", SQL.fieldValue(row.bairro));
		sql = sql.replace("%tipo_resp", SQL.fieldValue(row.tipo_resp));
		sql = sql.replace("%grupo_fat", SQL.fieldValue(row.grupo_fat));
		sql = sql.replace("%sit_imovel", SQL.fieldValue(row.sit_imovel));
		sql = sql.replace("%indi_med", SQL.fieldValue(row.indi_med));
		sql = sql.replace("%sit_agua", SQL.fieldValue(row.sit_agua));
		sql = sql.replace("%dt_lig_esgoto", SQL.fieldValue(row.dt_lig_esgoto));
		sql = sql.replace("%bac_esgoto", SQL.fieldValue(row.bac_esgoto));
		sql = sql.replace("%sit_esgoto", SQL.fieldValue(row.sit_esgoto));
		sql = sql.replace("%perc_esg", SQL.fieldValue(row.perc_esg));
		sql = sql.replace("%cons_med", SQL.fieldValue(row.cons_med));
		sql = sql.replace("%hidrometro", SQL.fieldValue(row.hidrometro));
		sql = sql.replace("%loc_hidr", SQL.fieldValue(row.loc_hidr));
		sql = sql.replace("%rot_ent", SQL.fieldValue(row.rot_ent));
		sql = sql.replace("%ord_ent", SQL.fieldValue(row.ord_ent));
		sql = sql.replace("%rot_leit", SQL.fieldValue(row.rot_leit));
		sql = sql.replace("%ord_leit", SQL.fieldValue(row.ord_leit));
		sql = sql.replace("%g_consumidor", SQL.fieldValue(row.g_consumidor));
		sql = sql.replace("%codabast_altern", SQL.fieldValue(row.codabast_altern));
		sql = sql.replace("%dt_ligacao_agua", SQL.fieldValue(row.dt_ligacao_agua));
		sql = sql.replace("%rot_mcp", SQL.fieldValue(row.rot_mcp));
		sql = sql.replace("%seq_mcp", SQL.fieldValue(row.seq_mcp));
		sql = sql.replace("%ddd", SQL.fieldValue(row.ddd));
		sql = sql.replace("%telefone", SQL.fieldValue(row.telefone));
		sql = sql.replace("%cep", SQL.fieldValue(row.cep));
		sql = sql.replace("%tipo_pessoa", SQL.fieldValue(row.tipo_pessoa));
		sql = sql.replace("%cpf_cnpj", SQL.fieldValue(row.cpf_cnpj));
		sql = sql.replace("%num_morad", SQL.fieldValue(row.num_morad));
		sql = sql.replace("%cod_resp", SQL.fieldValue(row.cod_resp));
		sql = sql.replace("%tipo_documento", SQL.fieldValue(row.tipo_documento));
		sql = sql.replace("%num_documento", SQL.fieldValue(row.num_documento));
		return sql;
	}

	protected void arquivarEsgotoSeMudou(Row1800 row) throws IOException {
		String sql = "select arquivar_esgoto(matricula, %datager) from dof_usuario "
				+ " where matricula = %mat "
				+ "   and (perc_esg <> %perc_esg or sit_esgoto <> %sit_esg);";
		sql = sql.replace("%mat", SQL.fieldValue(row.matricula));
		sql = sql.replace("%datager", SQL.fieldValue(parser.dataGeracaoArquivo));
		sql = sql.replace("%perc_esg", SQL.fieldValue(row.perc_esg));
		sql = sql.replace("%sit_esg", SQL.fieldValue(row.sit_esgoto));

		try {
			parser.query(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	protected void atualizaLeitura(int matricula, Leitura leitura, Referencia ref)
			throws SQLException, IOException {
		// log("Atualizando leitura de " + matricula);

		String sql = "select upsert_dof_fatura_leitura(%matricula, %ano, %mes, %cod_anl, %leitura, %consumo, %updl, %force);";

		sql = assignParametersLeitura(sql, matricula, leitura, ref);
		// Integer i = (Integer) singleValueQuery(sql);
		parser.singleValueQuery(sql);

		// if (i == 0)
		// System.out
		// .println("Importação para dof_fatura ignorada porque a leitura "
		// + ref.mes
		// + "/"
		// + ref.ano
		// + " na base está mais atualizada do que o arquivo");

	}

	protected void atualizaLeitura(int matricula, Leitura leitura, Referencia ref, int shiftRef)
			throws SQLException, IOException {
		if (leitura == null)
			return;
		ref = ref.copy();
		ref.shift(shiftRef);
		atualizaLeitura(matricula, leitura, ref);
	}

	protected void importaLogradouro(Row1800 row) {
		try {
			Statement s = connection.createStatement();

			String sql = "update dof_logradouro set " + "tipo = %tipo, " + "titulo = %titulo, "
					+ "nome = %nome " + " where localidade = %localidade and codigo = %codigo;";
			sql = assignParametersLogradouro(sql, row);
			s.execute(sql);
			int u = s.getUpdateCount();
			if (u > 0)
				return;

			sql = "insert into dof_logradouro (localidade, codigo, tipo, titulo, nome) "
					+ "values (%localidade, %codigo, %tipo, %titulo, %nome);";
			sql = assignParametersLogradouro(sql, row);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	protected void importaSetor(int codLocalidade, int setor, int grupo) {
		try {
			Statement s = connection.createStatement();

			String sql = "insert into dof_setor (localidade, setor, grupo) "
					+ "values (%localidade, %setor, %grupo);";
			sql = assignParametersSetor(sql, codLocalidade, setor, grupo);
			s.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	protected String assignParametersSetor(String sql, int localidade, int setor, int grupo) {
		sql = sql.replace("%localidade", SQL.fieldValue(localidade));
		sql = sql.replace("%setor", SQL.fieldValue(setor));
		sql = sql.replace("%grupo", SQL.fieldValue(grupo));
		sql = sql.replace("%datager", SQL.fieldValue(parser.dataGeracaoArquivo));
		return sql;
	}

	protected String assignParametersSetor(String sql, int localidade, int setor) {
		sql = sql.replace("%localidade", SQL.fieldValue(localidade));
		sql = sql.replace("%setor", SQL.fieldValue(setor));
		sql = sql.replace("%datager", SQL.fieldValue(parser.dataGeracaoArquivo));
		return sql;
	}

	protected String assignParametersLogradouro(String sql, Row1800 row) {
		sql = sql.replace("%localidade", SQL.fieldValue(row.codLocalidade));
		sql = sql.replace("%codigo", SQL.fieldValue(row.cod_lograd));
		sql = sql.replace("%tipo", SQL.fieldValue(row.tipo_lograd));
		sql = sql.replace("%titulo", SQL.fieldValue(row.titulo_lograd));
		sql = sql.replace("%nome", SQL.fieldValue(row.endereco));
		return sql;
	}

	protected Categoria findCategoria(ArrayList<Categoria> todo, Integer codCategoria) {
		for (Categoria c : todo) {
			if (c.categoria == codCategoria)
				return c;
		}
		return null;
	}

	protected void atualizaConsMedFatura(Row1800 row) throws IOException {
		String sql = "update dof_fatura set cons_med = %cons_med "
				+ "where (matricula, ano, mes) = (%matricula, %ano, %mes);";

		sql = sql.replace("%matricula", SQL.fieldValue(row.matricula));
		sql = sql.replace("%ano", SQL.fieldValue(row.ref_atual.ano));
		sql = sql.replace("%mes", SQL.fieldValue(row.ref_atual.mes));
		sql = sql.replace("%cons_med", SQL.fieldValue(row.cons_med));

		parser.execSQL(sql);

	}

	public void registrarInicioProcessamento(Localidade localidade, Date dataGeracaoArquivo) throws IOException {
		if (localidade != null) { 
			String sql = " insert into dof_importacao_1800(id, localidade, inicio, geracao_arquivo) "
					   + " values (nextval('dof_importacao_1800_id_seq'), %loc, now(), %arq) ";
			sql = sql.replace("%loc", SQL.fieldValue(localidade.localidade));
			sql = sql.replace("%arq", SQL.fieldValue(dataGeracaoArquivo));
			parser.execSQL(sql);
		}
	}
	
	public void registrarFimProcessamento(Localidade localidade, Date dataGeracaoArquivo) throws IOException {
		if (localidade != null) { 
			String sql = "update dof_importacao_1800 set final = now() "
					+ " where localidade = %loc and geracao_arquivo = %arq ";
			sql = sql.replace("%loc", SQL.fieldValue(localidade.localidade));
			sql = sql.replace("%arq", SQL.fieldValue(dataGeracaoArquivo));
			parser.execSQL(sql);
		}
	}


}
