package com.mysql.connect;

import java.sql.Timestamp;

public class HiveInstance {
	private String queryid = "";
	private String type = "";
	private String otid = "";
	private java.sql.Timestamp submittime;
	private String detail = "";
	private String status = "";
	private String sql = "";

	public HiveInstance(String queryid, String type, String otid,
			Timestamp submittime, String detail, String status, String sql) {
		super();
		this.queryid = queryid;
		this.type = type;
		this.otid = otid;
		this.submittime = submittime;
		this.detail = detail;
		this.status = status;
		this.sql = sql;
	}

	public String getQueryid() {
		return queryid;
	}

	public void setQueryid(String queryid) {
		this.queryid = queryid;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getOtid() {
		return otid;
	}

	public void setOtid(String otid) {
		this.otid = otid;
	}

	public java.sql.Timestamp getSubmittime() {
		return submittime;
	}

	public void setSubmittime(java.sql.Timestamp submittime) {
		this.submittime = submittime;
	}

	public String getDetail() {
		return detail;
	}

	public void setDetail(String detail) {
		this.detail = detail;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

}
