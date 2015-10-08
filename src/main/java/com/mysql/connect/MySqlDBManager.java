package com.mysql.connect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import util.ConfigContext;

public class MySqlDBManager {
	private static final String URL = ConfigContext.getInstance().getString(
			"mysql.url");
	private static final String USER = ConfigContext.getInstance().getString(
			"mysql.user");
	private static final String PSW = ConfigContext.getInstance().getString(
			"mysql.psw");

	public Connection conn = null;

	public static MySqlDBManager getInstance() {
		return InnerHolder.INSTANCE;
	}

	private MySqlDBManager() {
		this.init(URL, USER, PSW);
	}

	private static class InnerHolder {
		static final MySqlDBManager INSTANCE = new MySqlDBManager();
	}

	public boolean init(final String url, final String user, final String psw) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(url, user, psw);
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
		}
		return false;
	}

	public boolean execSQL(String sql) {
		boolean execResult = false;

		Statement statement = null;

		try {
			statement = conn.createStatement();
			if (statement != null) {
				execResult = statement.execute(sql);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			execResult = false;
		}

		return execResult;
	}

	public PreparedStatement creatPreparedStatement(String sql)
			throws SQLException {

		PreparedStatement pstmt = conn.prepareStatement(sql);

		return pstmt;
	}

	/**
	 * 测试方法---忽略
	 * 
	 * @param conn
	 * @param sql
	 */
	public static void test(Connection conn, String sql) {

		if (conn == null) {
			return;
		}

		Statement statement = null;
		ResultSet result = null;

		try {
			statement = conn.createStatement();
			result = statement.executeQuery(sql);
			if (result != null && result.first()) {
				int idColumnIndex = result.findColumn("opid");
				int nameColumnIndex = result.findColumn("status");
				while (!result.isAfterLast()) {
					System.out.println("------------------");
					System.out.print("id " + result.getString(idColumnIndex)
							+ "\t");
					System.out.println("name "
							+ result.getString(nameColumnIndex));
					result.next();
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (result != null) {
					result.close();
					result = null;
				}
				if (statement != null) {
					statement.close();
					statement = null;
				}

			} catch (SQLException sqle) {

			}
		}
	}

}
