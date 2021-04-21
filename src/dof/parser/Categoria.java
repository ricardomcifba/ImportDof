package dof.parser;

public class Categoria {
	
	public int categoria;
	public int economias;
	
	public Categoria(int categoria, int economias) {
		super();
		this.categoria = categoria;
		this.economias = economias;
	}

	@Override
	public String toString() {
		return getCategoriaParcial() + "." + getSubcategoria() + "." + economias;
	}

	private int getCategoriaParcial() {
		int c = categoria / 10;
		return c;
	}

	private int getSubcategoria() {
		int s = categoria - (getCategoriaParcial() * 10);
		return s;
	}

}
