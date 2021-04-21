package dof.bridge;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import dof.util.SQL;

public abstract class Bridge {

	private Connection source;

	protected Connection target;

	protected int globalRowId;

	public Bridge(Connection source, Connection target) {
		super();
		this.source = source;
		this.target = target;
	}

	protected void processRow(int rowId, ResultSet rs) throws SQLException {
		// nada
	}

	
	public void perform() throws SQLException, IOException {
		String sql = getSourceSQL();
		globalRowId = 0;
		int rowId;
		do {
			rowId = 0;
			ResultSet rs = SQL.query(source, sql);
			while (rs.next()) {
				rowId++;
				globalRowId++;
				processRow(rowId, rs);
			}

		} while (shouldRepeat(rowId, globalRowId));
	}

	protected abstract boolean shouldRepeat(int rowId, int globalRowId);

	protected abstract String getSourceSQL();

	public void assignSpecificParams(String[] params) {
		// nada
	}

}
