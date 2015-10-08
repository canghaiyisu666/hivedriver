package com.udps.hive.test;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCancelOperationReq;
import org.apache.hive.service.cli.thrift.TCancelOperationResp;
import org.apache.hive.service.cli.thrift.TGetLogReq;
import org.apache.hive.service.cli.thrift.TGetLogResp;
import org.apache.hive.service.cli.thrift.TGetOperationStatusReq;
import org.apache.hive.service.cli.thrift.TGetOperationStatusResp;
import org.apache.hive.service.cli.thrift.TOpenSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

import sun.misc.BASE64Decoder;

import com.mysql.connect.MySqlDBDao;

public class CancelTest {
	public static void main(String[] args) throws Exception {
		// cancel("192.168.8.203",
		// "hive_20150327143939_6868079f-c94e-4521-99fd-7d8ae1287856");
		getStatus();
	}

	/**
	 * 获取sql执行状态的封装方法
	 * 
	 * @return
	 * @throws TException
	 * @throws SQLException
	 * @throws IOException
	 */
	public static String getStatus() throws TException, SQLException,
			IOException {
		TSocket transport = new TSocket("172.16.8.222", 10000);
		TOperationHandle stmtHandle = new TOperationHandle();
		TCLIService.Client client = new TCLIService.Client(new TBinaryProtocol(
				transport));
		transport.open();

		MySqlDBDao dao = new MySqlDBDao();
		String queryid = "hive_20150826162828_c57815b4-27cb-48cf-80f1-08902ae233d5";
		String rs = dao.queryDetailById(queryid);

		byte[] stmt = new byte[256];
		BASE64Decoder dec = new BASE64Decoder();
		try {
			stmt = dec.decodeBuffer(rs);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		TDeserializer de = new TDeserializer();

		System.out.println("stmtHandle " + stmtHandle);
		de.deserialize(stmtHandle, stmt);
		System.out.println("stmtHandle " + stmtHandle);
		// ////////////////////////////

		TGetLogReq req1 = new TGetLogReq(stmtHandle);
		TGetLogResp log = client.GetLog(req1);
		System.out.println(log.getLog());

		TGetOperationStatusReq req = new TGetOperationStatusReq();
		req.setOperationHandle(stmtHandle);
		TGetOperationStatusResp statusResp = client.GetOperationStatus(req);
		System.out
				.println("status====" + statusResp.getOperationState().name());

		// ////////////////////////////
		transport.close();
		return null;
	}

	/**
	 * 静态的方法，用来取消正在执行的hive任务。
	 * 
	 * @param queryid
	 * @throws SQLException
	 * @throws TException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static void cancel(String ip, String queryid) throws SQLException,
			IOException, ClassNotFoundException {

		TSocket transport = new TSocket(ip, 10000);
		TCLIService.Client client = new TCLIService.Client(new TBinaryProtocol(
				transport));
		try {
			transport.open();
			System.out.println("transport opened");
			TOpenSessionReq openReq = new TOpenSessionReq();
			System.out.println("    	TOpenSessionReq ");
			TOpenSessionResp openResp = client.OpenSession(openReq);
			System.out.println("    	TOpenSessionResp ");
			TSessionHandle sessHandle = openResp.getSessionHandle();
			System.out.println("    	TSessionHandle ");
			TCancelOperationReq cancelReq = new TCancelOperationReq();
			TOperationHandle stmtHandle = new TOperationHandle();

			// 从数据库中的记录构建请求参数对象↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
			MySqlDBDao dao = new MySqlDBDao();
			String rs = dao.queryDetailById(queryid);

			byte[] stmt = new byte[256];
			BASE64Decoder dec = new BASE64Decoder();
			try {
				stmt = dec.decodeBuffer(rs);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			TDeserializer de = new TDeserializer();

			System.out.println("stmtHandle " + stmtHandle);
			de.deserialize(stmtHandle, stmt);
			System.out.println("stmtHandle " + stmtHandle);
			// 从数据库中的记录构建请求参数对象↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑

			cancelReq.setOperationHandle(stmtHandle);
			TCancelOperationResp cancelResp = client.CancelOperation(cancelReq);
			System.out.println(cancelResp.getStatus());

			// dao.deleteById(queryid);//取消hive任务之后删除mysql中的记录

			// TCloseOperationReq closeReq = new TCloseOperationReq();
			// closeReq.setOperationHandle(stmtHandle);
			// client.CloseOperation(closeReq);
			// TCloseSessionReq closeConnectionReq = new TCloseSessionReq(
			// sessHandle);
			// client.CloseSession(closeConnectionReq);

			transport.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
