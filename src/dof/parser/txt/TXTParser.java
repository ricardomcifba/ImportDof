package dof.parser.txt;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.Date;

import dof.parser.Parser;

public abstract class TXTParser extends Parser {

	protected int rowStartOffset = 0;

	protected Integer lastProcessedRow = null;

	private boolean restoredSavePoint = false;

	private File savePointFile;

	private Long lastSavePointTime;

	private Integer lastFieldPos;

	private Integer lastFieldSize;

	private String lastFieldContent;

	public static boolean STOP_ON_ERROR = true;

	public static String ENCODING = null;

	private BufferedWriter errorsBufferedWriter;

	private static int generalRowIndex = 0;

	public TXTParser(Connection connection, String fileName, String[] args) {
		super(connection, fileName, args);
	}

	@Override
	public final void process(boolean renameToDone) throws IOException {

		if (!shouldProcessThisFile())
			return;

		try {
			consumeSavePointFile();

			internalPerform(renameToDone);

			if (fileWasRenamedToDone())
				deleteSavePointFile();

		} catch (Exception e) {

			if (lastFieldContent != null)
				System.out.println("Ultimo campo extraído: '" + lastFieldContent + "' Pos="
						+ lastFieldPos + " Size=" + lastFieldSize);

			try {
				savePointIntoFile(e);
			} catch (Exception e1) {
				e1.printStackTrace();
			}

			throw new RuntimeException(e);
		}

		if (errorsBufferedWriter != null)
			errorsBufferedWriter.close();

		afterFinished();

	}

	private void deleteSavePointFile() {
		savePointFile.delete();
	}

	protected void consumeSavePointFile() throws IOException {
		String name = savePointFileName();
		savePointFile = new File(name);
		if (!savePointFile.exists())
			return;

		readSavePointFile(savePointFile);

		if (lastSavePointTime != null)
			restoredSavePoint = true;

	}

	private void readSavePointFile(File f) throws FileNotFoundException, IOException {
		FileReader fr = new FileReader(f);
		BufferedReader r = new BufferedReader(fr);
		try {
			while (r.ready()) {
				String s = r.readLine();

				if (s.startsWith("lastProcessedRow=")) {
					String[] ss = s.split("=");
					lastProcessedRow = Integer.parseInt(ss[1]);
				} else {
					readSavePointAdditionalInfo(s);
				}
			}
		} finally {
			r.close();
		}
	}

	protected void readSavePointAdditionalInfo(String s) {
		// sobrescrever em classes inferiores
	}

	protected void internalPerform(boolean renameToDone) throws Exception {
		BufferedReader reader;
		FileInputStream fis = null;

		if (ENCODING == null) {
			reader = new BufferedReader(new FileReader(fileName));
		} else {
			fis = new FileInputStream(fileName);
			InputStreamReader isr = new InputStreamReader(fis, ENCODING);
			reader = new BufferedReader(isr);
		}

		String content = null;
		int rowIndex = 0;
		boolean restoreSavePoint = false;
		if (lastProcessedRow != null) {
			restoreSavePoint = true;
		}
		try {
			if (!restoreSavePoint)
				System.out.println("Iniciando processamento em " + getFileNameToShow());
			else
				System.out.println("Continuando processamento em " + getFileNameToShow());

			do {
				if (restoreSavePoint) {
					while (rowIndex < lastProcessedRow) {
						content = reader.readLine();
						rowIndex++;
						generalRowIndex++;
					}
					restoreSavePoint = false;
				}

				if (shouldSavePoint(rowIndex))
					savePointIntoFile(null);

				content = reader.readLine();

				if (content == null)
					continue;

				if (finished)
					return;

				if (SHOW_SPENT_TIMES)
					checkAndShowSpentTimes(rowIndex);

				String rowInfo = rowIndex + "";
				if (rowIndex != generalRowIndex)
					rowInfo += "-" + generalRowIndex;
				String pre = getFileNameToShow() + "[" + rowInfo + "]: ";

				System.out.println(pre + content);

				try {

					processRow(rowIndex, content);

				} catch (IOException e) {

					throw e;

				} catch (Exception e) {

					if (!STOP_ON_ERROR) {
						saveIntoErrorFile(content);
						e.printStackTrace();

					} else {
						throw e;
					}
				}

				if (rowContainsAllRequiredData(rowIndex, content))
					markRowAsProcessed(rowIndex);
				rowIndex++;
				generalRowIndex++;

			} while (content != null);

			finished = true;
		} finally {

			processAfterLastRow(finished);

			if (fis != null)
				fis.close();

			reader.close();
			if (finished && renameToDone && fileIsValid)
				renameToDone();

			showSpentTimeByAction();
		}
	}

	protected void processAfterLastRow(boolean finished) {
		// sobrescrever em classes descendentes
	}

	private void saveIntoErrorFile(String content) throws IOException {

		if (errorsBufferedWriter == null) {
			File file = new File(this.fileName + ".errors");
			if (!file.exists())
				file.createNewFile();

			if (!file.canWrite())
				throw new RuntimeException("Não é permitido escrever no arquivo de erros");

			errorsBufferedWriter = new BufferedWriter(new FileWriter(file, true));
			String header = getHeader();
			if (header != null) {
				errorsBufferedWriter.write(header);
				errorsBufferedWriter.newLine();
				errorsBufferedWriter.flush();
			}
		}

		errorsBufferedWriter.write(content);
		errorsBufferedWriter.newLine();
		errorsBufferedWriter.flush();

	}

	protected String getHeader() {
		return null;
	}

	private void checkAndShowSpentTimes(long rowIndex) {
		if (rowIndex == 100 * (rowIndex / 100))
			showSpentTimeByAction();
	}

	private boolean shouldSavePoint(long rowId) {
		Long elapsed = elapsedTimeSinceLastSavePoint();
		if (elapsed == null)
			return true;
		if (elapsed > 30000)
			return true;
		return false;
	}

	private Long elapsedTimeSinceLastSavePoint() {
		Date now = new Date();
		if (lastSavePointTime == null)
			return null;
		return now.getTime() - lastSavePointTime;
	}

	/**
	 * Indica que esta linha, por si só, contém todas as informações que ela
	 * precisa para realizar a importação, sem depender de dados de nenhuma
	 * outra linha anterior. É empregado para o arquivo de SavePoint, pra evitar
	 * que, numa ocasião de "restore", ele continue de uma linha não-atômica e a
	 * importação fique inconsistente.
	 * 
	 * Sobrescrever nas classes descendentes.
	 */
	protected abstract boolean rowContainsAllRequiredData(int rowId, String content);

	protected String extract(String content, int pos, int size) {
		if (content == null)
			return null;
		pos += rowStartOffset;
		while (pos + size > content.length()) {
			size--;
			if (size < 0)
				return null;
		}

		String s = content.substring(pos, pos + size);
		setLastExtractedField(s, pos, size);
		return s;
	}

	private void setLastExtractedField(String content, int pos, int size) {
		this.lastFieldContent = content;
		this.lastFieldPos = pos;
		this.lastFieldSize = size;
	}

	protected void savePointIntoFile(Exception error) throws IOException {
		String spFileName = savePointFileName();
		File f = new File(spFileName);
		if (!f.exists())
			f.createNewFile();

		FileWriter w = new FileWriter(f);
		try {
			if (lastProcessedRow != null)
				w.write("lastProcessedRow=" + lastProcessedRow + "\r\n");
			if (error != null)
				w.write("errorMsg=" + error.getMessage() + "\r\n");

			writeAdditionalSavePointInfo(w);
		} finally {
			w.close();
		}

		lastSavePointTime = (new Date()).getTime();
	}

	protected void writeAdditionalSavePointInfo(FileWriter w) {
		// sobrescrever em classes descendentes

	}

	private String savePointFileName() {
		String spFileName = getFullPath() + ".sav";
		return spFileName;
	}

	protected void markRowAsProcessed(int rowId) {
		lastProcessedRow = rowId;
	}

	public boolean restoredSavePoint() {
		return restoredSavePoint;
	}



}
