package dof.parser.txt.F4200;

import java.io.IOException;
import java.sql.Connection;

import dof.parser.txt.TXTParser;

public class CIAF4200Parser extends TXTParser {


	private Importer4200 importer;

	public CIAF4200Parser(Connection connection, String fileName, String[] args) {
		super(connection, fileName, args);
		importer = new Importer4200(this, connection);
	}

	@Override
	protected void processRow(int rowId, String content) throws IOException {
		super.processRow(rowId, content);
		Row4200 row = parseLancRow(content);

		importer.doImport(row);
	}

	private Row4200 parseLancRow(String content) {
		Row4200 row = new Row4200();
		String s = extract(content, 0, 10);
		row.matricula = valorLong(s);

		s = extract(content, 10, 4);
		row.ano = valorInt(s);

		row.incFaturasAnoAtual();

		s = extract(content, 14, 2);
		row.mes = valorInt(s);

		s = extract(content, 16, 6);
		row.leitura = valorInt(s);

		s = extract(content, 22, 2);
		row.anl = valorInt(s);

		s = extract(content, 24, 6);
		row.consumo = valorInt(s);

		s = extract(content, 30, 2);
		row.anc = s;

		s = extract(content, 32, 2);
		row.diasConsumo = valorInt(s);

		s = extract(content, 34, 6);
		row.volumeFaturado = valorInt(s);

		s = extract(content, 40, 10);
		row.valorAgua = valorMonetario(s);

		row.confirmaVolumeFaturado();

		s = extract(content, 50, 10);
		row.valorEsgoto = valorMonetario(s);

		s = extract(content, 60, 10);
		row.valorServicos = valorMonetario(s);

		s = extract(content, 70, 1);
		if (s != null) {
			s = s.trim();
			if (s.equals(""))
				s = null;
		}
		row.baixa = s;
		return row;
	}

	@Override
	protected boolean rowContainsAllRequiredData(int rowId, String content) {
		return true;
	}
}
