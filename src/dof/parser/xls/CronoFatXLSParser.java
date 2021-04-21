package dof.parser.xls;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.ss.usermodel.Cell;

import dof.util.SQL;

public class CronoFatXLSParser extends XLSParser {

	protected String[] columnNames = new String[50];

	private Integer ano;
	private Integer mes;

	private Integer headerRow1;
	private Integer headerRow2;
	private Integer headerRow3;

	private String[] grupos;
	
	private List<String> alreadyProcessed = new ArrayList<String>();

	public CronoFatXLSParser(Connection connection, String fileName,
			String[] args) {
		super(connection, fileName, args);
		this.ignoreHiddenSheet = true;
	}

	@Override
	protected void processRow(HSSFRow row, int rowIndex, HSSFSheet sheet) throws IOException {

		if (rowIndex == 4) {
			headerRow1 = rowIndex;
			parseHeader1(row);
			return;
		}
		if (rowIndex == 5) {
			headerRow2 = rowIndex;
			parseHeader2(row);
			return;
		}
		if (rowIndex == 6) {
			headerRow3 = rowIndex;
			parseHeader3(row);
			return;
		}

		if (headerRow3 != null) {
			processDataRow(row, rowIndex);
		}
	}

	@Deprecated
	protected void processRow_Old(HSSFRow row, int rowIndex, HSSFSheet sheet) throws IOException {
		HSSFCell c = row.getCell(0);
		if (nullValue(c))
			return;

		if (c.getCellType() == Cell.CELL_TYPE_STRING) {
			String s = getCellValueAsString(c);
			s = s.trim();

			if (s.equals("GRUPO") || s.equals("GRUPOS")) {
				if (headerRow1 == null) {
					headerRow1 = rowIndex;
					parseHeader1(row);
					return;
				}
				if (headerRow2 == null) {
					headerRow2 = rowIndex;
					parseHeader2(row);
					return;
				}
				if (headerRow3 == null) {
					headerRow3 = rowIndex;
					parseHeader3(row);
					return;
				}
				throw new RuntimeException(
						"Layout do arquivo difere do esperado (muitos níveis de cabeçalho)");

			}
		}

		if (headerRow3 != null) {
			processDataRow(row, rowIndex);
		}
	}

	private String getCellValueAsString(HSSFCell c) {
		int cellType = c.getCellType();
		switch (cellType) {
		case HSSFCell.CELL_TYPE_STRING:
			return c.getStringCellValue();

		case HSSFCell.CELL_TYPE_NUMERIC:
			return c.getNumericCellValue() + "";

		case HSSFCell.CELL_TYPE_FORMULA:
			return c.getStringCellValue();

		}
		throw new RuntimeException("Tipo não suportado: " + cellType);
	}

	private void parseHeader1(HSSFRow row) {
		Iterator<Cell> cells = row.cellIterator();

		int lastColIndex = lastColIndex(cells);

		String previousName = null;
		for (int i = 0; i <= lastColIndex; i++) {
			Cell c = row.getCell(i);
			if (c.getCellType() == Cell.CELL_TYPE_STRING) {
				String s = c.getStringCellValue();
				parseHeader1Column(s, i);
				previousName = s;
			} else if (c.getCellType() == Cell.CELL_TYPE_BLANK) {
				if (previousName != null) {
					parseHeader1Column(previousName, i);
				}
			}
		}
	}

	private void parseHeader2(HSSFRow row) {
		Iterator<Cell> cells = row.cellIterator();

		int lastColIndex = lastColIndex(cells);

		String previousName = null;
		for (int i = 0; i <= lastColIndex; i++) {
			Cell c = row.getCell(i);
			if (c.getCellType() == Cell.CELL_TYPE_STRING) {
				String s = c.getStringCellValue();
				parseHeader2Column(s, i);
				previousName = s;
			} else if (c.getCellType() == Cell.CELL_TYPE_BLANK) {
				if (previousName != null) {
					parseHeader2Column(previousName, i);
				}
			}
		}
		// while (cells.hasNext()) {
		// Cell c = cells.next();
		// parseHeader2Column(c, colIndex);
		// colIndex++;
		// }
	}

	private int lastColIndex(Iterator<Cell> cells) {
		int r = 0;
		while (cells.hasNext()) {
			Cell c = cells.next();
			r = c.getColumnIndex();
		}
		return r;
	}

	private void parseHeader1Column(String name, int colIndex) {
		if (columnNames[colIndex] != null)
			return;
		String column = extractColumnNameHeader1(name, colIndex);
		columnNames[colIndex] = column;
	}

	private void parseHeader2Column(String name, int colIndex) {
		if (columnNames[colIndex] != null)
			return;
		String column = extractColumnNameHeader2(name, colIndex);
		columnNames[colIndex] = column;
	}

	private String extractColumnNameHeader1(String s, int colIndex) {
		if (sameHeaderLabel(s, "LEITURAS"))
			return null;
		if (sameHeaderLabel(s, "CONTAS"))
			return null;
		if (sameHeaderLabel(s, "GRUPO"))
			return null;
		if (sameHeaderLabel(s, "GRUPOS"))
			return null;
		if (sameHeaderLabel(s,
				"ÚLTIMA DATA PARA INCLUSÕES, EXCLUSÕES E ALTERAÇÕES"))
			return "conv_retiffim_%";
		if (sameHeaderLabel(s,
				"ÚLTIMA DATA DE INCLUSÕES"))
			return "conv_retiffim_%";
		if (sameHeaderLabel(s, "VENCIMENTO DAS CONTAS"))
			return "vencimento";
		if (sameHeaderLabel(s, "SETOR DE FATURAMENTO"))
			return null;
		if (sameHeaderLabel(s, "SETOR DE ABASTECIMENTO"))
			return null;
		if (sameHeaderLabel(s, "EMBASA - FTM"))
			return null;
		if (sameHeaderLabel(s, "EMBASA - FDI"))
			return null;
		if (sameHeaderLabel(s, "EMBASA - UNIDADE REGIONAL"))
			return null;
		if (sameHeaderLabel(s, "EMBASA - UNIDADE DE NEGOCIOS"))
			return null;
		if (sameHeaderLabel(s, "EMBASA - MAC"))
			return null;
		if (sameHeaderLabel(s, "EMBASA - FCA"))
			return null;
		throw new RuntimeException("Coluna não reconhecida: " + s);
	}

	private String extractColumnNameHeader2(String s, int colIndex) {
		if (sameHeaderLabel(s, "GERAÇÃO ARQUIVO"))
			return "geracaoarq";
		if (sameHeaderLabel(s, "PROC. ARQUIVOS RETORNO E ATUALIZAÇÃO GERENCIAL"))
			return "processarq%";
		if (sameHeaderLabel(s,
				"PROC. ARQUIVOS, RETORNO E ATUALIZAÇÃO GERENCIAL"))
			return "processarq%";
		if (sameHeaderLabel(s, "EXECUÇÃO"))
			return "leitura";
		if (sameHeaderLabel(s, "ANÁLISE WEBROL"))
			return "analise1";
		if (sameHeaderLabel(s, "DEVOLUÇÃO ARQUIVOS"))
			return "devolucaoarq%";
		if (sameHeaderLabel(s, "MANUTENÇÃO DE HIDRÔMETRO"))
			return "manuthidr%";
		if (sameHeaderLabel(s, "INCLUSÃO, ATUALIZAÇÃO E EXCLUSÃO"))
			return "retif%";
		if (sameHeaderLabel(s, "ANÁLISE CONTAS RETIDAS")) {
			if (colIndex == 7)
				return "analise1";
			if (colIndex == 15)
				return "analise2%";
			if (colIndex == 16)
				return "analise2%";
			if (colIndex == 17)
				return "analise2%";
			throw new RuntimeException("Posição inesperada para ANÁLISE CONTAS RETIDAS: " + colIndex);
		}
		if (sameHeaderLabel(s, "VENCIMENTO DAS CONTAS"))
			return "vencimento";
		if (sameHeaderLabel(s, "DIA DA EXECUÇÃO"))
			return "leitura";
		if (sameHeaderLabel(s, "LOC. INFORMATIZADA"))
			return "%_li";
		if (sameHeaderLabel(s, "LOC. NÃO INFORMATIZADA"))
			return "%_lni";
		if (sameHeaderLabel(s,
				"ÚLTIMA DATA PARA INCLUSÕES, EXCLUSÕES E ALTERAÇÕES"))
			return "conv_retiffim_%";
		if (sameHeaderLabel(s, "DATA EMISSÃO"))
			return "conv_emissao";
		if (sameHeaderLabel(s, "DATA DE EMISSÃO"))
			return "conv_emissao";
		if (sameHeaderLabel(s, "ÚLTIMO DIA DA ENTREGA"))
			return "conv_ud_entrega";
		if (sameHeaderLabel(s, "DATA DO VENCIMENTO"))
			return "vencimento";

		if (sameHeaderLabel(s, "GRUPO"))
			return null;
		if (sameHeaderLabel(s, "GRUPOS"))
			return null;
		if (sameHeaderLabel(s, "SETOR DE FATURAMENTO"))
			return null;
		if (sameHeaderLabel(s, "SETOR DE ABASTECIMENTO"))
			return null;
		if (sameHeaderLabel(s, "PREPA-RAÇÃO"))
			return null;
		if (sameHeaderLabel(s, "PREPARAÇÃO"))
			return null;
		throw new RuntimeException("Coluna não reconhecida: " + s);
	}

	private boolean sameHeaderLabel(String s1, String s2) {
		s1 = s1.replace(" ", "").trim();
		s2 = s2.replace(" ", "").trim();
		return s1.equalsIgnoreCase(s2);
	}

	private void parseHeader3(HSSFRow row) {

		replaceWildCardColumns("processarq%", "processarqini", "sem coluna",
				"processarqfim");
		replaceWildCardColumns("devolucaoarq%", "devolucaoarqini",
				"sem coluna", "devolucaoarqfim");
		replaceWildCardColumns("manuthidr%", "manuthidrfim", "manuthidrreini");
		replaceWildCardColumns("retif%", "retiffim", "retifreini");
		replaceWildCardColumns("analise2%", "analise2ini", "sem coluna",
				"analise2fim");
		replaceWildCardColumns("%_li", "conv_digitacao_li", "conv_analise_li");
		replaceWildCardColumns("%_lni", "conv_analise_el_lni",
				"conv_digitacao_lni", "conv_analise_ur_lni");
		replaceWildCardColumns("conv_retiffim_%", "conv_retiffim_li",
				"conv_retiffim_lni");

	}

	private void replaceWildCardColumns(String wildCard, String... columns) {
		int j = 0;
		for (int i = 0; i < columnNames.length; i++) {
			if (j >= columns.length)
				return;

			String c = columnNames[i];
			if (c != null)
				if (c.equals(wildCard)) {
					columnNames[i] = columns[j];
					j++;
				}
		}
	}

	protected void processDataRow(HSSFRow row, int rowIndex) throws IOException {
		if (shouldIgnoreRow(row, rowIndex))
			return;

		Iterator<Cell> cells = row.cellIterator();
		int colIndex = 0;

		grupos = extractGrupos(row.getCell(0));
		if (grupos.length == 0)
			return;

//		deleteGrupos(grupos);
		insertLinhasInexistentesNoCronograma(grupos);

		while (cells.hasNext()) {
			String colName = columnNames[colIndex];
			HSSFCell cell = (HSSFCell) cells.next();
			colIndex++;
			if (colName == null)
				continue;

			importData(colName, cell);
		}
	}

	private boolean shouldIgnoreRow(HSSFRow row, int rowIndex) {
		HSSFCell c = row.getCell(1);
		
		int t;
		try {
			t = c.getCellType();
		} catch (Exception e) {
			// Significa que não existe coluna 1. Neste caso, ignorar sim.
			return true;
		}
		if (!(t == Cell.CELL_TYPE_STRING))
			return false;
		String s = c.getStringCellValue().trim();
		if (s.equalsIgnoreCase("PROCESSAMENTO DAS LOCALIDADES/SETORES COM FATURAMENTO SUSPENSO"))
			return true;
		if (s.equalsIgnoreCase("PROCESSAMENTO DAS LOCALIDADES /SETOR COM FATURAMENTO SUSPENSO"))
			return true;
		if (s.equalsIgnoreCase("PROCESSAMENTO DAS LOCALIDADES/SETOR COM FATURAMENTO SUSPENSO"))
			return true;
		if (s.equalsIgnoreCase("ÓRGÃOS PÚBLICOS"))
			return true;
		return false;
	}

	private void insertLinhasInexistentesNoCronograma(String[] grupos) throws IOException {
		for (String g : grupos) {
			String sql = "select * from dof_cronogramafat where grupo = %grupo and mes = %mes and ano = %ano;";
			sql = assignContextParams(g, sql);
			
			try {
			
				if (!emptyQuery(sql))
					continue;
				
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
			
			sql = "insert into dof_cronogramafat (grupo, mes, ano) values (%grupo, %mes, %ano);";
			sql = assignContextParams(g, sql);
			
				execSQL(sql);
		}
	}

	private String assignContextParams(String g, String sql) {
		sql = sql.replace("%grupo", g);
		sql = sql.replace("%mes", mes + "");
		sql = sql.replace("%ano", ano + "");
		return sql;
	}

	protected void deleteGrupos(String[] grupos) throws IOException {
		String sql = "delete from dof_cronogramafat where " + contexto() + ";";
		execSQL(sql);
	}

	private String[] extractGrupos(HSSFCell c) {
		
		String s;
		try {
			s = stringCellValue(c);
		} catch (Exception e2) {
			System.out.println("Erro na linha " + c.getRowIndex() + " / coluna "
					+ c.getColumnIndex());
			throw new RuntimeException(e2);
		}
		if (s == null)
			return new String[0];

		s = s.replaceAll("\\*", "");
		String[] ss = s.split("\\-");
		for (String e : ss) {
			try {
				Integer.parseInt(e);
			} catch (NumberFormatException e1) {
				System.out.println("Grupos não numéricos: " + s);
				return new String[0];
			}
		}
		return ss;
	}

	private String stringCellValue(HSSFCell c) {
		int t = c.getCellType();
		if (t == Cell.CELL_TYPE_STRING)
			return c.getStringCellValue();

		if (t == Cell.CELL_TYPE_NUMERIC) {
			int r = (int) c.getNumericCellValue();
			return r + "";
		}
		if (t == Cell.CELL_TYPE_BLANK)
			return null;

		if (t == Cell.CELL_TYPE_FORMULA) {
			try {
				return c.getStringCellValue();
			} catch (Exception e) {
				return ((int) c.getNumericCellValue()) + "";
			}
		}
		System.out.println("Erro na linha " + c.getRowIndex() + " / coluna "
				+ c.getColumnIndex());
		throw new RuntimeException("Situação inesperada");
	}

	private void importData(String colName, HSSFCell cell) throws IOException {
		if (colName.equals("sem coluna"))
			return;

		Date d = null;
		try {
			d = getDateValue(cell);
		} catch (Exception e) {
			System.out.println("Erro na linha " + cell.getRowIndex()
					+ " / coluna " + cell.getColumnIndex());

			for (int i = 0; i < columnNames.length; i++) {
				System.out.println("[" + i + "]" + columnNames[i]);
			}

			throw new RuntimeException(e);
		}
		
		if (d == null) {
			return;
		}
		
		String sql = "update dof_cronogramafat set " + colName + " = "
				+ SQL.fieldValue(d) + " where " + contexto() + ";";

		execSQL(sql);
		
		markAsAlreadyProcessed(contexto(), colName);
	}

	private void markAsAlreadyProcessed(String contexto, String colName) {
		String s = contexto + " " + colName;
		if (alreadyProcessed.contains(contexto))
			throw new RuntimeException("Item já importado: " + s);
		alreadyProcessed.add(s);
	}

	private Date getDateValue(HSSFCell cell) {
		// if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC)
		return cell.getDateCellValue();
		// return null;
	}

	private String contexto() {
		return "ano = " + SQL.fieldValue(ano) + " and mes = " + SQL.fieldValue(mes)
				+ " and grupo in (" + byComma(grupos) + ")";
	}

	protected String contexto(String grupo) {
		return "ano = " + SQL.fieldValue(ano) + " and mes = " + SQL.fieldValue(mes)
				+ " and grupo = " + grupo + ";";
	}

	private String byComma(String[] ss) {
		String r = "";
		for (int i = 0; i < ss.length; i++) {
			if (r.equals(""))
				r = r + ss[i];
			else
				r = r + ", " + ss[i];
		}
		return r;
	}

	@Override
	protected boolean beforeProcessSheet(HSSFSheet sheet, int sheetIndex) {
		clearContextVars();
		String name = sheet.getSheetName();
		if (name.equals("Plan1"))
			return false;
		if (name.equalsIgnoreCase("Feriados"))
			return false;

		parseMesAno(name);

		return true;
	}

	private void clearContextVars() {
		headerRow1 = null;
		headerRow2 = null;
		headerRow3 = null;
		ano = null;
		mes = null;
	}

	private void parseMesAno(String name) {
		name = name.trim();
		String[] ss = name.split(" ");
		if (ss.length != 2)
			throw new RuntimeException(
					"Não foi possível extrair a referência do nome da Sheet: "
							+ name);

		mes = parseMonth(ss[0]);
		ano = parseAno(ss[1]);
	}

	private int parseMonth(String s) {
		if (s.equalsIgnoreCase("Jan"))
			return 1;
		if (s.equalsIgnoreCase("Fev"))
			return 2;
		if (s.equalsIgnoreCase("Mar"))
			return 3;
		if (s.equalsIgnoreCase("Abr"))
			return 4;
		if (s.equalsIgnoreCase("Mai"))
			return 5;
		if (s.equalsIgnoreCase("Jun"))
			return 6;
		if (s.equalsIgnoreCase("Jul"))
			return 7;
		if (s.equalsIgnoreCase("Ago"))
			return 8;
		if (s.equalsIgnoreCase("Set"))
			return 9;
		if (s.equalsIgnoreCase("Out"))
			return 10;
		if (s.equalsIgnoreCase("Nov"))
			return 11;
		if (s.equalsIgnoreCase("Dez"))
			return 12;
		throw new RuntimeException("Mês inválido: " + s);
	}

	private int parseAno(String s) {
		return Integer.parseInt(s);
	}

}
