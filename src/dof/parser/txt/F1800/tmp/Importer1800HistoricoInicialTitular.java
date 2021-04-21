package dof.parser.txt.F1800.tmp;

import java.io.IOException;
import java.sql.Connection;

import dof.parser.txt.F1800.CIAF1800Parser;
import dof.parser.txt.F1800.Importer1800;
import dof.parser.txt.F1800.Row1800;

public class Importer1800HistoricoInicialTitular extends Importer1800 {

	public Importer1800HistoricoInicialTitular(CIAF1800Parser parser, Connection connection) {
		super(parser, connection);
	}

	@Override
	public void internalImport(int rowIndex, Row1800 row) throws IOException {
		arquivarTitular(row.matricula, row.cpf_cnpj, row.nome);
	}

}
