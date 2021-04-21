package dof.parser.txt.F4200;


public class Row4200 {

	Long matricula;
	Integer ano;

	Integer anoAnt;
	private int faturasAnoAtual = 0;
	private int faturasBCNAnoAtual = 0;

	Integer mes;
	Integer leitura;
	Integer anl;
	Integer consumo;
	String anc;

	Integer diasConsumo;
	Integer volumeFaturado;
	public Row4200() {
		super();
	}

	Double valorAgua;
	Double valorEsgoto;
	Double valorServicos;
	String baixa;

	boolean registroValido() {
		if ((valorAgua == null) && (valorEsgoto == null) && (valorServicos == null)
				&& (consumo == null) && (anl == null) && (leitura == null))
			return false;
		return true;
	}

	void confirmaVolumeFaturado() {
		if ((volumeFaturado != null) && (volumeFaturado != null))
			if ((volumeFaturado > 0) && (valorAgua == 0.0)) {
				System.out.println("Aviso: volume faturado " + volumeFaturado
						+ " sem valor de água. Corrigindo para volume faturado zero.");
				volumeFaturado = 0;
			}
	}

	@Override
	public String toString() {
		return "Row4200 [matricula=" + matricula + ", ano=" + ano + ", mes=" + mes
				+ ", leitura=" + leitura + ", anl=" + anl + ", consumo=" + consumo + ", anc=" + anc
				+ ", diasConsumo=" + diasConsumo + ", volumeFaturado=" + volumeFaturado
				+ ", valorAgua=" + valorAgua + ", valorEsgoto=" + valorEsgoto + ", valorServicos="
				+ valorServicos + "]";
	}

	public int getFaturasAnoAtual() {
		return faturasAnoAtual;
	}

	public void setFaturasAnoAtual(int faturasAnoAtual) {
		this.faturasAnoAtual = faturasAnoAtual;
	}

	public int getFaturasBCNAnoAtual() {
		return faturasBCNAnoAtual;
	}

	public void setFaturasBCNAnoAtual(int faturasBCNAnoAtual) {
		this.faturasBCNAnoAtual = faturasBCNAnoAtual;
	}

	public void incFaturasAnoAtual() {
		faturasAnoAtual++;
		
		if (BCN(baixa)) {
			faturasBCNAnoAtual++;
		}

		if ((anoAnt == null)||(!anoAnt.equals(ano))) {
			anoAnt = ano;
			faturasAnoAtual = 0;
			faturasBCNAnoAtual = 0;
		}
	}


	private static boolean BCN(String b) {
		if (b == null)
			return false;
		if (b.equals("B"))
			return true;
		if (b.equals("C"))
			return true;
		if (b.equals("N"))
			return true;
		return false;
	}

}
