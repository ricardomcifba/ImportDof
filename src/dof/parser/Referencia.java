package dof.parser;

public class Referencia {
	
	public int ano;
	public int mes;
	public Referencia(int ano, int mes) {
		super();
		this.ano = ano;
		this.mes = mes;
	}
	@Override
	public String toString() {
		return mes + "/" + ano;
	}
	public Referencia copy() {
		return new Referencia(ano, mes);
	}
	public void shift(int meses) {
		mes += meses;
		while (mes < 1) {
			ano -= 1;
			mes += 12;
		};

		while (mes > 12) {
			ano += 1;
			mes -= 12;
		};
	}

	
}
