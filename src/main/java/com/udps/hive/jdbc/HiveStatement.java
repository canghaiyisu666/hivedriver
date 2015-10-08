package com.udps.hive.jdbc;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCancelOperationReq;
import org.apache.hive.service.cli.thrift.TCancelOperationResp;
import org.apache.hive.service.cli.thrift.TCloseOperationReq;
import org.apache.hive.service.cli.thrift.TCloseOperationResp;
import org.apache.hive.service.cli.thrift.TExecuteStatementReq;
import org.apache.hive.service.cli.thrift.TExecuteStatementResp;
import org.apache.hive.service.cli.thrift.TFetchOrientation;
import org.apache.hive.service.cli.thrift.TFetchResultsReq;
import org.apache.hive.service.cli.thrift.TFetchResultsResp;
import org.apache.hive.service.cli.thrift.TGetLogReq;
import org.apache.hive.service.cli.thrift.TGetLogResp;
import org.apache.hive.service.cli.thrift.TGetOperationStatusReq;
import org.apache.hive.service.cli.thrift.TGetOperationStatusResp;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import sun.misc.BASE64Encoder;

import com.mysql.connect.HiveInstance;
import com.mysql.connect.MySqlDBDao;

/**
 * HiveStatement.
 *
 */
public class HiveStatement implements java.sql.Statement {
	private final HiveConnection connection;
	private TCLIService.Iface client;
	private TOperationHandle stmtHandle = null;
	private final TSessionHandle sessHandle;
	Map<String, String> sessConf = new HashMap<String, String>();
	private int fetchSize = 50;
	private boolean isScrollableResultset = false;
	/**
	 * We need to keep a reference to the result set to support the following:
	 * <code>
	 * statement.execute(String sql);
	 * statement.getResultSet();
	 * </code>.
	 */
	private ResultSet resultSet = null;

	/**
	 * Sets the limit for the maximum number of rows that any ResultSet object
	 * produced by this Statement can contain to the given number. If the limit
	 * is exceeded, the excess rows are silently dropped. The value must be >=
	 * 0, and 0 means there is not limit.
	 */
	private int maxRows = 0;

	/**
	 * Add SQLWarnings to the warningChain if needed.
	 */
	private SQLWarning warningChain = null;

	/**
	 * Keep state so we can fail certain calls made after close().
	 */
	private boolean isClosed = false;

	// A fair reentrant lock
	private ReentrantLock transportLock = new ReentrantLock(true);

	public HiveStatement(HiveConnection connection, TCLIService.Iface client,
			TSessionHandle sessHandle) {
		this(connection, client, sessHandle, false);
	}

	public HiveStatement(HiveConnection connection, TCLIService.Iface client,
			TSessionHandle sessHandle, boolean isScrollableResultset) {
		this.connection = connection;
		this.client = client;
		this.sessHandle = sessHandle;
		this.isScrollableResultset = isScrollableResultset;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#addBatch(java.lang.String)
	 */

	@Override
	public void addBatch(String sql) throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#cancel()
	 */
	@Override
	public void cancel() throws SQLException {
		if (isClosed) {
			throw new SQLException(
					"Can't cancel after statement has been closed");
		}

		if (stmtHandle == null) {
			return;
		}

		TCancelOperationReq cancelReq = new TCancelOperationReq();
		cancelReq.setOperationHandle(stmtHandle);
		try {
			transportLock.lock();
			TCancelOperationResp cancelResp = client.CancelOperation(cancelReq);
			Utils.verifySuccessWithInfo(cancelResp.getStatus());
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new SQLException(e.toString(), "08S01", e);
		} finally {
			transportLock.unlock();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#clearBatch()
	 */

	@Override
	public void clearBatch() throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#clearWarnings()
	 */

	@Override
	public void clearWarnings() throws SQLException {
		warningChain = null;
	}

	void closeClientOperation() throws SQLException {
		try {
			if (stmtHandle != null) {
				TCloseOperationReq closeReq = new TCloseOperationReq();
				closeReq.setOperationHandle(stmtHandle);
				transportLock.lock();
				TCloseOperationResp closeResp = client.CloseOperation(closeReq);
				Utils.verifySuccessWithInfo(closeResp.getStatus());
			}
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new SQLException(e.toString(), "08S01", e);
		} finally {
			transportLock.unlock();
		}
		stmtHandle = null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#close()
	 */
	@Override
	public void close() throws SQLException {
		if (isClosed) {
			return;
		}
		if (stmtHandle != null) {
			closeClientOperation();
		}
		client = null;
		resultSet = null;
		isClosed = true;
	}

	public void closeOnCompletion() throws SQLException {
		// JDK 1.7
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#execute(java.lang.String)
	 */

	@Override
	public boolean execute(String sql) throws SQLException {
		if (isClosed) {
			throw new SQLException(
					"Can't execute after statement has been closed");
		}

		// // ↓↓↓↓↓↓↓↓↓↓↓↓UDF加前缀操作↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
		MySqlDBDao dao = new MySqlDBDao();
		// ArrayList<String> udfname = dao.findUdfs();
		// if (udfname.size() != 0) {
		// for (String string : udfname) {
		// if (sql.contains(string)) {
		// sql = sql.replace(string, "default." + string);
		// }
		// }
		// }
		// // ↑↑↑↑↑↑↑↑↑↑↑↑UDF加前缀操作↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓

		try {
			if (stmtHandle != null) {
				closeClientOperation();
			}

			TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle,
					sql);
			/**
			 * Run asynchronously whenever possible Currently only a
			 * SQLOperation can be run asynchronously, in a background operation
			 * thread Compilation is synchronous and execution is asynchronous
			 */
			execReq.setRunAsync(true);
			execReq.setConfOverlay(sessConf);
			transportLock.lock();

			TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
			Utils.verifySuccessWithInfo(execResp.getStatus());
			stmtHandle = execResp.getOperationHandle();

			// ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓得到queryId，打印相关日志信息↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
			TGetLogReq req = new TGetLogReq(stmtHandle);
			TGetLogResp log = client.GetLog(req);
			
//			BufferedReader rdr = new BufferedReader(new StringReader(
//					log.getLog()));
//			List<String> lines = new ArrayList<String>();
//			for (String line = rdr.readLine(); line != null; line = rdr
//					.readLine()) {
//				lines.add(line);
//			}
//			rdr.close();
//			System.out.println("com.udps.hive.jdbc.HiveDriver===>EXPLAIN生成抽象语法树如下：");
//			for (String line : lines) {
//				if (!line.isEmpty()) {
//					String regEx = "[0-9][0-9]/[0-9][0-9]/[0-9][0-9]";
//					Pattern pat = Pattern.compile(regEx);
//					Matcher mat = pat.matcher(line);
//					if (mat.find()) {
//					} else {
//						System.out.println("com.udps.hive.jdbc.HiveDriver===>   "
//								+ line);
//					}
//				}
//			}
//			System.out.println();

			String[] tmp = log.getLog().split("queryid ");
			if (tmp.length > 1) {// 当取到queryid时再到数据库存储记录。
				String[] tmptmp = tmp[1].split(" :");
				String queryid = tmptmp[0];
				System.out.println("com.udps.hive.jdbc.HiveDriver===>"
						+ "queryid:" + queryid);
				// ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑得到queryId↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑

				String otid = "";
				if (System.getenv("otid") != null
						&& System.getenv("otid").length() != 0) {
					otid = System.getenv("otid");
				}
				System.out.println("com.udps.hive.jdbc.HiveDriver===>"
						+ "otid:" + otid);

				// 将本次hive请求保存到mysql数据库，以便取消任务，查询状态
				TSerializer serializer = new TSerializer(
						new TBinaryProtocol.Factory());
				byte[] stmt = serializer.serialize(stmtHandle);

				BASE64Encoder enc = new BASE64Encoder();
				String encodedString = enc.encode(stmt); // 通过编码算法将byte[]转换为string保存数据库

				String sqlTmp=sql.replaceAll("'", "#");
				
				HiveInstance hi = new HiveInstance(queryid, stmtHandle
						.getOperationType().toString(), otid,
						new java.sql.Timestamp(new java.util.Date().getTime()),
						encodedString, "RUNNING_STATE", sqlTmp);

				dao.insertHiveinstance(hi);
				System.out.println("com.udps.hive.jdbc.HiveDriver===>"
						+ "本次sql句柄已保存。");
			}
			
			
			

		} catch (SQLException eS) {
			throw eS;
		} catch (Exception ex) {
			throw new SQLException(ex.toString(), "08S01", ex);
		} finally {
			transportLock.unlock();
		}

		TGetOperationStatusReq statusReq = new TGetOperationStatusReq(
				stmtHandle);
		boolean operationComplete = false;
		TGetOperationStatusResp statusResp;

		// Poll on the operation status, till the operation is complete
		while (!operationComplete) {
			try {
				/**
				 * For an async SQLOperation, GetOperationStatus will use the
				 * long polling approach It will essentially return after the
				 * HIVE_SERVER2_LONG_POLLING_TIMEOUT (a server config) expires
				 */
				transportLock.lock();
				try {
					statusResp = client.GetOperationStatus(statusReq);
				} catch (Exception e) {
					throw e;
				} finally {
					transportLock.unlock();
				}
				Utils.verifySuccessWithInfo(statusResp.getStatus());
				if (statusResp.isSetOperationState()) {
					switch (statusResp.getOperationState()) {
					case CLOSED_STATE:
					case FINISHED_STATE:
						operationComplete = true;
						break;
					case CANCELED_STATE:
						// 01000 -> warning
						throw new SQLException("Query was cancelled", "01000");
					case ERROR_STATE:
						// Get the error details from the underlying exception
						throw new SQLException(statusResp.getErrorMessage(),
								statusResp.getSqlState(),
								statusResp.getErrorCode());
					case UKNOWN_STATE:
						throw new SQLException("Unknown query", "HY000");
					case INITIALIZED_STATE:
					case PENDING_STATE:
					case RUNNING_STATE:
						break;
					}
				}
			} catch (SQLException e) {
				throw e;
			} catch (Exception e) {
				throw new SQLException(e.toString(), "08S01", e);
			}
		}

		// The query should be completed by now
		if (!stmtHandle.isHasResultSet()) {
			return false;
		}
		resultSet = new HiveQueryResultSet.Builder(this).setClient(client)
				.setSessionHandle(sessHandle).setStmtHandle(stmtHandle)
				.setMaxRows(maxRows).setFetchSize(fetchSize)
				.setScrollable(isScrollableResultset).build();
		return true;
	}
	
	
	
	
//	 public List<String> getQueryLog(int fetchSize)
//		      throws Exception {
//		  
//		    List<String> logs = new ArrayList<String>();
//		    TFetchResultsResp tFetchResultsResp = null;
//		    try {
//		        TFetchResultsReq tFetchResultsReq = new TFetchResultsReq(stmtHandle,
//		        		TFetchOrientation.FETCH_NEXT, fetchSize);//TFetchOrientation.FETCH_FIRST
//		        tFetchResultsResp = client.FetchResults(tFetchResultsReq);
//		        Utils.verifySuccessWithInfo(tFetchResultsResp.getStatus());
//		    } catch (SQLException e) {
//		      throw e;
//		    } catch (Exception e) {
//		      throw new SQLException("Error when getting query log: " + e, e);
//		    }
//
//		    RowSet rowSet = RowSetFactory.create(tFetchResultsResp.getResults(),
//		        connection.getProtocol());
//		    for (Object[] row : rowSet) {
//		      logs.add(String.valueOf(row[0]));
//		    }
//		    return logs;
//		  }
	
	
	

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#execute(java.lang.String, int)
	 */

	@Override
	public boolean execute(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw new SQLException("Method not supported");
	}

	public String getLog() throws SQLException {
		if (isClosed) {
			throw new SQLException(
					"Can't get log for statement after statement has been closed");
		}

		TGetLogReq getLogReq = new TGetLogReq();
		TGetLogResp getLogResp;
		getLogReq.setOperationHandle(stmtHandle);
		try {
			getLogResp = client.GetLog(getLogReq);
			Utils.verifySuccessWithInfo(getLogResp.getStatus());
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw new SQLException(e.toString(), "08S01", e);
		}
		return getLogResp.getLog();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#execute(java.lang.String, int[])
	 */

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
	 */

	@Override
	public boolean execute(String sql, String[] columnNames)
			throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#executeBatch()
	 */

	@Override
	public int[] executeBatch() throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#executeQuery(java.lang.String)
	 */

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		if (!execute(sql)) {
			throw new SQLException("The query did not generate a result set!");
		}
		return resultSet;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#executeUpdate(java.lang.String)
	 */

	@Override
	public int executeUpdate(String sql) throws SQLException {
		execute(sql);
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int)
	 */

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
	 */

	@Override
	public int executeUpdate(String sql, int[] columnIndexes)
			throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#executeUpdate(java.lang.String,
	 * java.lang.String[])
	 */

	@Override
	public int executeUpdate(String sql, String[] columnNames)
			throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getConnection()
	 */

	@Override
	public Connection getConnection() throws SQLException {
		return this.connection;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getFetchDirection()
	 */

	@Override
	public int getFetchDirection() throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getFetchSize()
	 */

	@Override
	public int getFetchSize() throws SQLException {
		return fetchSize;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getGeneratedKeys()
	 */

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getMaxFieldSize()
	 */

	@Override
	public int getMaxFieldSize() throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getMaxRows()
	 */

	@Override
	public int getMaxRows() throws SQLException {
		return maxRows;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getMoreResults()
	 */

	@Override
	public boolean getMoreResults() throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getMoreResults(int)
	 */

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getQueryTimeout()
	 */

	@Override
	public int getQueryTimeout() throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getResultSet()
	 */

	@Override
	public ResultSet getResultSet() throws SQLException {
		return resultSet;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getResultSetConcurrency()
	 */

	@Override
	public int getResultSetConcurrency() throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getResultSetHoldability()
	 */

	@Override
	public int getResultSetHoldability() throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getResultSetType()
	 */

	@Override
	public int getResultSetType() throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getUpdateCount()
	 */

	@Override
	public int getUpdateCount() throws SQLException {
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getWarnings()
	 */

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return warningChain;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#isClosed()
	 */

	@Override
	public boolean isClosed() throws SQLException {
		return isClosed;
	}

	public boolean isCloseOnCompletion() throws SQLException {
		// JDK 1.7
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#isPoolable()
	 */

	@Override
	public boolean isPoolable() throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setCursorName(java.lang.String)
	 */

	@Override
	public void setCursorName(String name) throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setEscapeProcessing(boolean)
	 */

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setFetchDirection(int)
	 */

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setFetchSize(int)
	 */

	@Override
	public void setFetchSize(int rows) throws SQLException {
		fetchSize = rows;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setMaxFieldSize(int)
	 */

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setMaxRows(int)
	 */

	@Override
	public void setMaxRows(int max) throws SQLException {
		if (max < 0) {
			throw new SQLException("max must be >= 0");
		}
		maxRows = max;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setPoolable(boolean)
	 */

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setQueryTimeout(int)
	 */

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
	 */

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLException("Method not supported");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Wrapper#unwrap(java.lang.Class)
	 */

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException("Method not supported");
	}

}
