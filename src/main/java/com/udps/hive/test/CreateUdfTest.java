package com.udps.hive.test;
//package com.udps.hive.test;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.Statement;
//
//public class CreateUdfTest {
//
//	public static void main(String[] args) throws Exception {
//		try {
//			//org.apache.hadoop.hive.jdbc.HiveDriver  hiveserver1驱动
//			
//			Class.forName("org.apache.hive.jdbc.HiveDriver");
//			// 现在是hiveserver2的驱动
//			// Class.forName("com.udps.hive.jdbc.HiveDriver");
//		} catch (Exception e) {
//			e.printStackTrace();
//			System.exit(1);
//		}
//		Connection con = DriverManager
//				.getConnection("jdbc:hive2://192.168.8.102:10000/;auth=noSasl",
//						"hive", "hive");
//		Statement stmt = con.createStatement();
//		
//		String SQL = "create function default.udf as 'com.udf.UdfTest' USING JAR 'hdfs:///home/msl/udf/UdfTest.jar' ";
//		
//		System.out.println("SQL=====>    " + SQL);
//		Boolean b = stmt.execute(SQL);
//
//		System.out.println("执行结果==="+b);
//		
////		PreparedStatement pst = con.prepareStatement();
////		pst.executeQuery();
//		// String SQL = "select count(*) from xm1.flumedb";
//		// String sql2 ="create function default.udf as 'com.udf.UdfTest'";
//		// ResultSet res = stmt.executeQuery(sql2);
////		while (res.next()) {
////			System.out.println(res.getString(1) + "\t");
////		}
//
//	}
//}
