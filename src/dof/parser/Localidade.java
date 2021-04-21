package dof.parser;

public class Localidade {
	
	public int diretoria;
	public int polo;
	public String ur;
	public int localidade;

	public Localidade() {
		super();
	}

	public boolean pertence(String escopo) {
		if (escopo.equals("EMBASA"))
			return true;
		if (escopo.equals("DM"))
			if (diretoria == 1)
				return true;
		if (escopo.equals("DN"))
			if (diretoria == 2)
				return true;
		if (escopo.equals("DS"))
			if (diretoria == 3)
				return true;
		if (escopo.equals(ur))
			return true;
		return false;
	}

}