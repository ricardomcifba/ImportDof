package dof;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import dof.bridge.Bridge;
import dof.bridge.view4200.Default4200Bridge;
import dof.parser.Parser;
import dof.util.FCFP;
import dof.util.SQL;

public class BridgeMain {

	private static Connection source;

	private static Connection target;

//	private static String[] mainArgs;

	private static String[] bridgeArgs;

	public static void main(String[] args) throws IOException, SQLException {
		extractArgs(args);

		source = FCFP.newConnectionFromFile("db2.conf");
		target = FCFP.newConnectionFromFile("db.conf");

		processBridge();

		System.out.println("Processamento concluído.");

	}

	private static void processBridge() throws SQLException, IOException {
		Bridge b = suitableBridge(bridgeArgs);
		System.out.println("Iniciando importação para " + toString(bridgeArgs));
		b.perform();
	}

	private static String toString(String[] ss) {
		String r = "";
		for (String s : ss)
			r += s + " ";
		return r;
	}

	private static Bridge suitableBridge(String[] params) {
		Bridge r = null;
		if (params.length == 0)
			throw new RuntimeException("Consulta não especificada");
			
		r = createBridge(params[0]);
		
		if (r != null)
			r.assignSpecificParams(params);
		
		return r;
	}

	private static Bridge createBridge(String type) {
		if (type.equals("4200"))
		return new Default4200Bridge(source, target);

		throw new RuntimeException("Tipo de consulta não suportado: " + type); 
	}

	private static void extractArgs(String[] args) {
//		mainArgs = args;

		ArrayList<String> ar = new ArrayList<String>();

		for (String s : args) {
			if (s.equals("LOG_SQL")) {
				SQL.LOG_SQL = true;
				continue;
			}

			if (s.equals("FORCE_UPDATE")) {
				Parser.FORCE_UPDATE = true;
				continue;
			}

			ar.add(s);
		}
		bridgeArgs = new String[ar.size()];
		bridgeArgs = ar.toArray(bridgeArgs);
	}

}
