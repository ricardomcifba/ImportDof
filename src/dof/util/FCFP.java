package dof.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class FCFP {
	public static Connection newConnectionFromFile(String filePath) throws IOException,
			SQLException {

		BufferedReader reader = new BufferedReader(new FileReader(filePath));

		String line = null;
		String url = null;
		String user = null;
		String password = null;

		try {
			// reader.readLine();
			do {
				line = reader.readLine();
				if (line == null)
					continue;
				line = line.trim();

				if (line.equals(""))
					continue;

				// if (line.startsWith("driver")) {
				// String ss[] = line.split("=");
				// r.driver = ss[1];
				// continue;
				// }

				if (line.startsWith("url")) {
					String ss[] = line.split("=");
					url = ss[1];
					continue;
				}

				if (line.startsWith("user")) {
					String ss[] = line.split("=");
					user = ss[1];
					if (user.equals("postgres"))
						if (password == null)
							password = "fcfp01";
					continue;
				}

				if (line.startsWith("password")) {
					String ss[] = line.split("=");
					password = ss[1];
					continue;
				}

				// if (line.startsWith("port")) {
				// String ss[] = line.split("=");
				// r.port = Integer.parseInt(ss[1]);
				// continue;
				// }

				// if (otherProperties != null) {
				// String ss[] = line.split("=");
				// if (ss.length == 0)
				// continue;
				// String key = ss[0];
				// if (ss.length > 2)
				// throw new RuntimeException("Inconsistência de propriedade: "
				// + key);
				// String value = null;
				// if (ss.length == 2)
				// value = ss[1];
				// otherProperties.put(key, value);
				// }

			} while (line != null);
			return DriverManager.getConnection(url, user, password);
		} finally {
			reader.close();
		}
	}

}
