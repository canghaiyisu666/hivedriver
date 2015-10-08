package com.mysql.connect;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
//public static void main(String[] args) throws SQLException {
//updateHiveinstance("1111", "status", "finished");
//}

public class MySqlDBDao {

	/**
	 * 到udf表中查询全部以后的udf名称
	 * 
	 * @return
	 * @throws SQLException
	 */
	public ArrayList<String> findUdfs() throws SQLException {
		ArrayList<String> list = new ArrayList<String>();
		String sql = "select udfname from udf";
		Statement statement = MySqlDBManager.getInstance().conn
				.createStatement();
		ResultSet result = statement.executeQuery(sql);
		while (result.next()) {
			list.add(result.getString("udfname"));
		}
		return list;
	}

	/**
	 * 添加hive示例记录到mysql
	 * 
	 * @param hi
	 *            实体类
	 */
	public void insertHiveinstance(HiveInstance hi) {
		// HiveInstance h = new HiveInstance("333333", "run", "exec",
		// new java.sql.Timestamp(new java.util.Date().getTime()),
		// "dddddddddddd");
		// insertHiveinstance(h);
		String sql = "insert into hiveinstance(queryid,type,otid,submittime,detail,status,sqlline) values('"
				+ hi.getQueryid()
				+ "','"
				+ hi.getType()
				+ "','"
				+ hi.getOtid()
				+ "','"
				+ hi.getSubmittime()
				+ "','"
				+ hi.getDetail()
				+ "','"
				+ hi.getStatus() 
				+ "','" 
				+ hi.getSql() 
				+ "')";
		MySqlDBManager.getInstance().execSQL(sql);

	}

	/**
	 * 将状态为finished的记录全部删除
	 */
	public void deleteHiveinstance() {
		String sql = "DELETE FROM hiveinstance WHERE status='finished'";
		MySqlDBManager.getInstance().execSQL(sql);
	}

	/**
	 * 通过ID删除mysql中的记录
	 */
	public void deleteById(String id) {
		String sql = "DELETE FROM hiveinstance WHERE queryid='" + id + "'";
		MySqlDBManager.getInstance().execSQL(sql);
	}

	/**
	 * 修改hive语句的执行状态
	 * 
	 * @param column
	 *            列名
	 * @param value
	 *            修改后的值
	 */
	public void updateHiveinstance(String id, String column, String value) {
		String sql = "update hiveinstance set " + column + "='" + value
				+ "' where queryid='" + id + "'";
		MySqlDBManager.getInstance().execSQL(sql);
	}

	/**
	 * 根据ID查询hive算子的状态
	 * 
	 * @param id
	 *            实例ID
	 * @return
	 * @throws SQLException
	 */
	public String queryStatusById(String id) throws SQLException {
		Statement statement = MySqlDBManager.getInstance().conn
				.createStatement();
		String sql = "select status from hiveinstance where queryid='" + id
				+ "'";
		ResultSet result = statement.executeQuery(sql);

		if (result != null && result.first()) {
			return result.getString("status");
		} else {
			return "Not found status in table hiveinstance";
		}
	}

	/**
	 * 根据ID得到statement对象JSON串
	 * 
	 * @throws IOException
	 */
	public String queryDetailById(String id) throws SQLException, IOException {
		Statement statement = MySqlDBManager.getInstance().conn
				.createStatement();
		String sql = "select detail from hiveinstance where queryid='" + id
				+ "'";
		ResultSet result = statement.executeQuery(sql);
		String detail = "";
		while (result.next()) {
			detail = result.getString("detail");
		}
		return detail;
	}

}
