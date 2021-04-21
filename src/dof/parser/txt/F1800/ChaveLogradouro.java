package dof.parser.txt.F1800;

@SuppressWarnings("rawtypes")
class ChaveLogradouro implements Comparable {
	int localidade;
	int codigo;

	public ChaveLogradouro(int localidade, int codigo) {
		super();
		this.localidade = localidade;
		this.codigo = codigo;
	}

	@Override
	public String toString() {
		return "ChaveLogradouro [localidade=" + localidade + ", codigo=" + codigo + "]";
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
		return 0;
	}

}

