package dof;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import dof.parser.Parser;
import dof.parser.txt.CTSSFCFPParser;
import dof.parser.txt.TXTParser;
import dof.parser.txt.F1800.CIAF1800Parser;
import dof.parser.txt.F4200.CIAF4200Parser;
import dof.parser.xls.AvalServXLSParser;
import dof.parser.xls.COPAEXLSParser;
import dof.parser.xls.CronoFatXLSParser;
import dof.util.FCFP;
import dof.util.SQL;
import dof.util.Util;

public class ImportDOF {

	// private static String workingDirOld = "c:/bd/dof/";

	//private static String workingDir = "d:/bd/dof/";
	
	/*
	 * alteração em 06 dezembro de 2017.
	 */
	private static String workingDir;

	private static Connection connection;

	private static ArrayList<String> prior1800list = prior1800List();

	private static ArrayList<String> dirsToImport = new ArrayList<String>();

	private static ArrayList<String> ignoredFileList = new ArrayList<String>();

	private static String[] mainArgs;

	private static String currentParserTag = null;

	private static boolean REPROCESSAR_DONES = false;
	
	private static long ARQUIVO_VAZIO = 662;

	public static void main(String[] args) throws IOException, SQLException {
		
		/* Alteração feita em 06 Dezembro de 2017 
		 * Problema foi devido a troca de servidor e a flexibilidade de caminho para a pasta
		 * */
		Properties props = new Properties();
		FileInputStream file = new FileInputStream("db.conf");
		props.load(file);
		workingDir = props.getProperty("dir_files");
		
		extractArgs(args);

		connection = FCFP.newConnectionFromFile("db.conf");

		String[] folders = listFolders();
		importFilesFromDirs(folders, true);
		System.out.println("Processamento concluído.");

	}

	private static void importFilesFromDirs(String[] dirs, boolean detectParserFromDirName)
			throws IOException, SQLException {
		for (String dir : dirs) {
			if (!shouldImportDir(dir))
				continue;
			if (detectParserFromDirName)
				detectParser(dir);

			boolean b = true;
			do {
				b = importFilesFromDir(dir);
			} while (b);

		}
	}

	private static String suitableParserName(String dir) {
		if (dir.contains("1800"))
			return "1800";
		if (dir.contains("4200"))
			return "4200";
		if (dir.contains("CTSS"))
			return "CTSS";
		if (dir.contains("COPAE"))
			return "COPAE";
		if (dir.contains("CRONOFAT"))
			return "CRONOFAT";
		if (dir.contains("AVALSERV"))
			return "AVALSERV";
		// if (dir.contains("ArquivarEsgoto"))
		// return "ArquivarEsgoto";

		return null;
	}

	private static void detectParser(String dir) {
		currentParserTag = suitableParserName(dir);
	}

	@Deprecated
	private static boolean shouldImportDir(String dir) {
		if (dir.endsWith("old"))
			return false;
		if (dir.contains("old\\")||dir.contains("old/"))
			return false;
		if (dir.endsWith("_lib"))
			return false;
		if (suitableParserName(dir) == null)
			return false;

		if (dirsToImport.size() == 0)
			return true;

		for (String d : dirsToImport) {
			if (dir.contains(d))
				return true;
		}
		return false;
	}

	private static void extractArgs(String[] args) {
		mainArgs = args;

		workingDir = Util.parseDefaultStringArg(args, "WORKING_DIR", workingDir);
		if (workingDir.equals("%CURRENT"))
			workingDir = System.getProperty("user.dir");
		// workingDir = workingDir.replace("\\", "/");

		SQL.LOG_SQL = Util.parseDefaultBooleanArg(args, "LOG_SQL", SQL.LOG_SQL);

		TXTParser.STOP_ON_ERROR = Util.parseDefaultBooleanArg(args, "STOP_ON_ERROR",
				TXTParser.STOP_ON_ERROR);

		Parser.FORCE_UPDATE = Util
				.parseDefaultBooleanArg(args, "FORCE_UPDATE", Parser.FORCE_UPDATE);

		TXTParser.ENCODING = Util.parseDefaultStringArg(args, "ENCODING", TXTParser.ENCODING);

		String[] dirOptions = new String[] { "1800", "4200", "COPAE", "CRONOFAT" };
		for (String s : args) {
			if (Util.contains(dirOptions, s))
				dirsToImport.add(s);
		}

	}

	protected static ArrayList<String> prior1800List() {
		ArrayList<String> r = new ArrayList<String>();

		r.add("GEO0700");
		r.add("GEO0900");
		r.add("GEO0901");
		r.add("GEO0902");
		r.add("GEO0903");

		return r;
	}

	private static String[] listFolders() {
		List<String> r = new ArrayList<String>();
		File d = new File(workingDir);

		if (!d.exists())
			throw new RuntimeException("Diretório não encontrado: " + workingDir);
		// d = new File(workingDirOld);

		File[] fs = d.listFiles();
		for (File f : fs) {
			if (!f.isDirectory())
				continue;
			String name = f.getName().toLowerCase();
			if (!isAcceptableName(name))
				continue;
			r.add(f.getAbsolutePath());
		}

		String[] ss = new String[0];
		return r.toArray(ss);
	}

	private static String[] listFolders(String rootPath) {
		List<String> r = new ArrayList<String>();
		File d = new File(rootPath);

		File[] fs = d.listFiles();
		for (File f : fs) {
			if (!f.isDirectory())
				continue;
			String name = f.getName().toLowerCase();
			if (!isAcceptableName(name))
				continue;
			r.add(f.getAbsolutePath());
		}

		Collections.sort(r);

		String[] ss = new String[0];

		return r.toArray(ss);
	}

	private static boolean importFilesFromDir(String dir) throws IOException, SQLException {
		System.out.println("Processando pasta " + dir);

		boolean r = false;

		String[] folders = listFolders(dir);
		importFilesFromDirs(folders, false);

		int total = 0;
		List<File> files = listFiles(dir);
		for (File f : files) {
			String fileName = f.getName();
			if (f.length() == (long) ARQUIVO_VAZIO) 
				continue;
			if (fileName.toLowerCase().endsWith(".sav"))
				// arquivos de SavePoint da importação. Contém, por ex., última
				// linha do arquivo a ser importado
				continue;
			if (fileName.toLowerCase().endsWith(".src"))
				// arquivos de parâmetro para a fonte de dados.
				// Ex.: CTSS, as SS's listadas do PG para serem baixadas do SCI
				continue;
			if (fileName.toLowerCase().endsWith(".bat"))
				continue;
			if (fileName.toLowerCase().endsWith(".errors"))
				continue;
			if (fileName.toLowerCase().endsWith(".ebm"))
				continue;
			if (fileName.toLowerCase().endsWith(".dll"))
				continue;
			if (fileName.toLowerCase().endsWith(".exe"))
				continue;
			if (fileName.toLowerCase().endsWith(".jar"))
				continue;
			if (fileName.toLowerCase().endsWith(".conf"))
				continue;
			if (Parser.endsWithDone(fileName))
				if (!REPROCESSAR_DONES)
					continue;
			if (endsWithHidden(fileName))
				continue;
			if (isIgnoredFile(f.getAbsolutePath()))
				continue;

			boolean p = perform(dir, fileName);

			r = true;
			if (p)
				total++;
			
			System.out.println("Arquivo "+fileName+", sendo processado.");
			
		}
		String plural = "";
		if (total != 1)
			plural = "s";
		System.out.println(total + " arquivo" + plural + " analisado" + plural + " na pasta " + dir);
		return r;
	}

	private static boolean isAcceptableName(String name) {
		if (name.startsWith("old") || name.endsWith("old"))
			return false;
		if (name.startsWith("hidden") || name.endsWith("hidden"))
			return false;
		return true;
	}

	private static boolean perform(String dir, String fileName) throws IOException, SQLException {
		Parser p = suitableParser(connection, dir, fileName);

		if (p == null)
			return true;

		p.process(true);

		if (!p.fileWasRenamedToDone()) {
			String path = p.getFullPath();
			addToIgnoreList(path);
		}

		return true;
	}

	private static void addToIgnoreList(String fullPath) {
		fullPath = fullPath.replace("/", "\\");
		ignoredFileList.add(fullPath);
	}

	private static boolean endsWithHidden(String fileName) {
		return fileName.toLowerCase().endsWith("hidden");
	}

	protected static boolean isPrior(File f, String directory) {
		if (directory.endsWith("1800")) {
			String name = f.getName();
			return isPriorFor1800(name);
			// throw new RuntimeException("Checar diretório: 1800 / " + name);
		}
		// TODO Auto-generated method stub
		return false;
	}

	protected static boolean isPriorFor1800(String fileName) {
		// if (prior1800list.contains(fileName))
		// return true;
		fileName = fileName.replace(".txt", "");
		if (prior1800list.contains(fileName))
			return true;
		return false;
	}

	protected static List<File> listFiles(final String directory) {
		File dir = new File(directory);
		if (!dir.exists())
			throw new RuntimeException("Diretório não existe: " + dir);
		File[] fs = dir.listFiles();

		ArrayList<File> ar = new ArrayList<File>();

		assert fs != null;
		for (File f : fs) {
			if (f.isDirectory())
				continue;
			if (!isAcceptableName(f.getName()))
				continue;
			ar.add(f);
		}

		sortFilesForImporting(directory, ar);
		return ar;
	}

	private static boolean isIgnoredFile(String fileName) {
		fileName = fileName.replace("/", "\\");
		return ignoredFileList.contains(fileName);
	}

	private static void sortFilesForImporting(final String directory, ArrayList<File> ar) {
		Collections.sort(ar, fileSortingComparator(directory));
	}

	@Deprecated
	protected static Comparator<File> fileSortingComparatorOld(final String directory) {
		return new Comparator<File>() {

			@Override
			public int compare(File f1, File f2) {
				if (isPrior(f1, directory) == isPrior(f2, directory))
					return compareDates(f1, f2);
				if (isPrior(f1, directory))
					return -1;
				if (isPrior(f2, directory))
					return 1;
				return compareDates(f1, f2);
			}

			private int compareDates(File f1, File f2) {
				if (f1.lastModified() < f2.lastModified())
					return -1;
				if (f1.lastModified() == f2.lastModified())
					return 0;
				return 1;
			}

		};
	}

	protected static Comparator<File> fileSortingComparatorByDate(final String directory) {
		return new Comparator<File>() {

			@Override
			public int compare(File f1, File f2) {
				return compareDates(f1, f2);
			}

			private int compareDates(File f1, File f2) {
				if (f1.lastModified() < f2.lastModified())
					return -1;
				if (f1.lastModified() == f2.lastModified())
					return 0;
				return 1;
			}

		};
	}

	private static Comparator<File> fileSortingComparator(final String directory) {
		if (directory.endsWith("1800"))
			return CIAF1800Parser.fileSortingComparatorFor1800(connection);
		return defaultFileSortingComparator(directory);
	}

	private static Comparator<File> defaultFileSortingComparator(final String directory) {
		return new Comparator<File>() {

			@Override
			public int compare(File f1, File f2) {

				if (isPrior(f1, directory) == isPrior(f2, directory))
					return compareDates(f1, f2);
				if (isPrior(f1, directory))
					return -1;
				if (isPrior(f2, directory))
					return 1;

				return compareDates(f1, f2);
			}

			private int compareDates(File f1, File f2) {
				if (f1.lastModified() < f2.lastModified())
					return -1;
				if (f1.lastModified() == f2.lastModified())
					return 0;
				return 1;
			}

		};
	}

	public static Parser suitableParser(Connection connection, String currentDir, String fileName)
			throws IOException, SQLException {
		fileName = currentDir + "/" + fileName;

		if (currentParserTag == null) {
			// throw new RuntimeException(
			// "Não foi identificado um parser adequado para " + currentDir);
			System.out.println("Não foi identificado um parser adequado para " + currentDir);
			return null;
		}

		if (currentParserTag.equals("4200"))
			return new CIAF4200Parser(connection, fileName, mainArgs);
		if (currentParserTag.equals("1800"))
			return new CIAF1800Parser(connection, fileName, mainArgs);
		if (currentParserTag.equals("CTSS"))
			return new CTSSFCFPParser(connection, fileName, mainArgs);
		if (currentParserTag.equals("COPAE"))
			return new COPAEXLSParser(connection, fileName, mainArgs);
		if (currentParserTag.equals("CRONOFAT"))
			return new CronoFatXLSParser(connection, fileName, mainArgs);
		if (currentParserTag.equals("AVALSERV"))
			return new AvalServXLSParser(connection, fileName, mainArgs);

		// if (currentParserTag.equals("ArquivarEsgoto"))
		// return new ArquivarEsgoto1800Parser(connection, fileName, mainArgs);

		throw new RuntimeException("Arquivo não suportado: " + fileName);
	}

}
