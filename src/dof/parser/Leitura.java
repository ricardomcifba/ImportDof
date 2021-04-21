package dof.parser;

public class Leitura {
	
	public int cod_anl;
	public int leitura;
	public int consumo;

	public Leitura(int cod_anl, int leitura, int consumo) {
		super();
		this.cod_anl = cod_anl;
		this.leitura = leitura;
		this.consumo = consumo;
	}

	@Override
	public String toString() {
		return "Leitura [cod_anl=" + cod_anl + ", leitura=" + leitura + ", consumo="
				+ consumo + "]";
	}

}
