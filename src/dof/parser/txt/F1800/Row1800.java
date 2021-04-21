package dof.parser.txt.F1800;

import java.util.ArrayList;
import java.util.Date;

import dof.parser.Categoria;
import dof.parser.Leitura;
import dof.parser.Localidade;
import dof.parser.Referencia;

public class Row1800 {

	Localidade localidade;
	public int matricula;
	public String nome;
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
	public Referencia ref_atual;
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
	public Long cpf_cnpj;
	int num_morad;
	int cod_clas_ppl;
	int cod_loc_ppl;
	int dig_cod_loc_ppl;
	String proc_jud;
	int cod_resp;
	Integer matricula_principal;
	String tipo_documento;
	String num_documento;
	int volume_faturado;
	String cod_anc;

	public ArrayList<Categoria> getCategorias() {
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

}