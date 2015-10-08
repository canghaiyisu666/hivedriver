package com.udps.hive.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveOperate {

	public static void main(String[] args) throws Exception {
		for (int i = 0; i < args.length; i++) {
			System.out.println("第"+i+"个参数是=========>"+args[i]);
			
		}
		try {
			// Class.forName("org.apache.hive.jdbc.HiveDriver");
			// 现在是hiveserver2的驱动
			Class.forName("com.udps.hive.jdbc.HiveDriver");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		String conString ="jdbc:hive2://172.16.8.222:10000/;auth=noSasl";
		Connection con = DriverManager.getConnection(
				conString);
		System.out.println();
		Statement stmt = con.createStatement();

//		String SQL = "select count(*) from xm1.flumedb";
		String SQL = "select count(*)  from test ";
		
		System.out.println("SQL=====>    " + SQL);
		ResultSet res = stmt.executeQuery(SQL);
		
		while (res.next()) {
			System.out.println(res.getString(1) + "\t");
		}

	}
}
