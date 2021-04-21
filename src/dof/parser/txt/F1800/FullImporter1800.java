package dof.parser.txt.F1800;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import dof.parser.Parser;

public class FullImporter1800 extends Importer1800 {

	public FullImporter1800(CIAF1800Parser parser, Connection connection) {
		super(parser, connection);
	}

	private ArrayList<Integer> setoresAtualizados = new ArrayList<Integer>();

	private List<ChaveLogradouro> logradourosAtualizados = new ArrayList<ChaveLogradouro>();

	@Override
	public void internalImport(int rowIndex, Row1800 row) throws IOException {

		if (!Parser.FORCE_UPDATE)
			if (matriculaIsAlreadyUpdated(row.matricula))
				return;

		atualizaSetor(row);
		atualizaLogradouro(row);

		parser.startMonitoringAction("Arquivando titularidade");
		arquivarTitularSeMudou(row);
		parser.endMonitoringAction("Arquivando titularidade");

		parser.startMonitoringAction("Arquivando situação/abast.altern de água");
		arquivarAguaSeMudou(row);
		parser.endMonitoringAction("Arquivando situação/abast.altern de água");

		parser.startMonitoringAction("Arquivando situação/percentual de esgoto");
		arquivarEsgotoSeMudou(row);
		parser.endMonitoringAction("Arquivando situação/percentual de esgoto");

		parser.startMonitoringAction("Importando usuario");
		atualizaUsuario(row);
		parser.endMonitoringAction("Importando usuario");

		parser.startMonitoringAction("Atualizando categoria");
		if (mudouCategoria(row))
			importaCategorias(row);
		parser.endMonitoringAction("Atualizando categoria");

		if (row.hidrometro != null) {
			parser.startMonitoringAction("Importando hidrometro");
			atualizaHidrometro(row);
			parser.endMonitoringAction("Importando hidrometro");

			parser.startMonitoringAction("Importando usuariohidrometro");
			atualizaUsuarioHidrometro(row);
			parser.endMonitoringAction("Importando usuariohidrometro");
		}

		if (row.ref_atual != null) {
			parser.startMonitoringAction("Importando fatura atual");
			atualizaFatura(row);
			parser.endMonitoringAction("Importando fatura atual");

			parser.startMonitoringAction("Importando Leituras");
			atualizaLeituras(row);
			parser.endMonitoringAction("Importando Leituras");
		}

		parser.startMonitoringAction("Importando cobrança");
		atualizaCobranca(row);
		parser.endMonitoringAction("Importando cobrança");

		markMatriculaAsUpdated(row.matricula);
	}

	private boolean logradouroAtualizado(int localidade, int cod_lograd) {
		return logradourosAtualizados.contains(new ChaveLogradouro(localidade, cod_lograd));
	}

	private boolean setorAtualizado(int setor) {
		return setoresAtualizados.contains(setor);
	}

	private void atualizaLeituras(Row1800 row) {
		try {
			atualizaLeitura(row.matricula, row.leitura1, row.ref_atual, 0);
			// atualizaLeitura(leitura2, -1);
			// atualizaLeitura(leitura3, -2);
			// atualizaLeitura(leitura4, -3);
			// atualizaLeitura(leitura5, -4);
			// atualizaLeitura(leitura6, -5);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private void atualizaLogradouro(Row1800 row) {
		parser.startMonitoringAction("Importando logradouro");
		if (!logradouroAtualizado(row.codLocalidade, row.cod_lograd)) {
			importaLogradouro(row);
			// System.out.println("atualizando logradouro " + codLocalidade +
			// ", "
			// + cod_lograd);
			ChaveLogradouro c = new ChaveLogradouro(row.codLocalidade, row.cod_lograd);
			logradourosAtualizados.add(c);
		}
		parser.endMonitoringAction("Importando logradouro");
	}

	private void atualizaSetor(Row1800 row) throws IOException {
		parser.startMonitoringAction("Importando setor");
		if (!setorAtualizado(row.setor)) {
			if (!existeSetorNaBase(row.codLocalidade, row.setor)) {
				importaSetor(row.codLocalidade, row.setor, row.grupo_fat);
				setoresAtualizados.add(row.setor);
			}
		}
		parser.endMonitoringAction("Importando setor");
	}


}
