package net.gywn.binlog.dbms;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import javax.sql.DataSource;

import lombok.Getter;
import lombok.Setter;
import net.gywn.binlog.api.TargetHandler;
import net.gywn.binlog.beans.BinlogOperation;
import net.gywn.binlog.beans.TargetOperation;
import net.gywn.binlog.common.UldraConfig;

public class UldraMysqlApplier implements TargetHandler {

	@Override
	public void init(final UldraConfig uldraConfig) throws Exception {
		System.out.println(this.getClass().getName() + " => initalized");
	}

	@Override
	public void insert(final Connection connection, final TargetOperation operation) throws SQLException {
		StringBuffer sbCol = new StringBuffer();
		StringBuffer sbVal = new StringBuffer();
		List<String> params = new ArrayList<String>();

		// gen sql
		for (Entry<String, String> e : operation.getDatMap().entrySet()) {
			if (sbCol.length() > 0) {
				sbCol.append(",");
				sbVal.append(",");
			}
			sbCol.append(e.getKey());
			sbVal.append("?");
			params.add(e.getValue());
		}

		String sql = String.format("insert into %s (%s) values (%s)\n", operation.getTableName(), sbCol.toString(),
				sbVal.toString());
//		System.out.println("UldraMysqlApplier=>insert");
//		System.out.printf(">> " + sql);
		executeUpdate(connection, sql, params);

	}

	@Override
	public void upsert(final Connection connection, final TargetOperation operation) throws SQLException {
		StringBuffer sbCol = new StringBuffer();
		StringBuffer sbVal = new StringBuffer();
		StringBuffer sbDup = new StringBuffer();
		List<String> params = new ArrayList<String>();

		// gen sql
		for (Entry<String, String> e : operation.getDatMap().entrySet()) {
			if (sbCol.length() > 0) {
				sbCol.append(",");
				sbVal.append(",");
				sbDup.append(",");
			}
			sbCol.append(e.getKey());
			sbVal.append("?");
			sbDup.append(String.format("%s=values(%s)", e.getKey(), e.getKey()));
			params.add(e.getValue());
		}

		String sql = String.format("insert ignore into %s (%s) values (%s) on duplicate key update %s\n",
				operation.getTableName(), sbCol.toString(), sbVal.toString(), sbDup.toString());
//		System.out.println("UldraMysqlApplier=>upsert");
//		System.out.printf(">> " + sql);
		executeUpdate(connection, sql, params);
	}

	@Override
	public void update(final Connection connection, final TargetOperation operation) throws SQLException {
		StringBuffer sbSet = new StringBuffer();
		StringBuffer sbWhe = new StringBuffer();
		List<String> params = new ArrayList<String>();

		// gen sql
		// set
		for (Entry<String, String> e : operation.getDatMap().entrySet()) {
			if (sbSet.length() > 0) {
				sbSet.append(",");
			}
			sbSet.append(String.format("%s=?", e.getKey()));
			params.add(e.getValue());
		}

		// where
		for (Entry<String, String> e : operation.getKeyMap().entrySet()) {
			if (sbWhe.length() == 0) {
				sbWhe.append("and ");
			}
			sbWhe.append(String.format("%s=?", e.getKey()));
			params.add(e.getValue());
		}

		String sql = String.format("update ignore %s set %s where 1=1 %s\n", operation.getTableName(), sbSet.toString(),
				sbWhe.toString());
//		System.out.println("UldraMysqlApplier=>update");
//		System.out.printf(">> " + sql);
		executeUpdate(connection, sql, params);

	}

	@Override
	public void delete(final Connection connection, final TargetOperation operation) throws SQLException {
		StringBuffer sbWhe = new StringBuffer();
		List<String> params = new ArrayList<String>();

		// gen sql
		// where
		for (Entry<String, String> e : operation.getKeyMap().entrySet()) {
			if (sbWhe.length() == 0) {
				sbWhe.append("and ");
			}
			sbWhe.append(String.format("%s=?", e.getKey()));
			params.add(e.getValue());
		}

		String sql = String.format("delete ignore from %s where 1=1 %s\n", operation.getTableName(), sbWhe.toString());
//		System.out.println("UldraMysqlApplier=>delete");
//		System.out.printf(">> " + sql);
		executeUpdate(connection, sql, params);
	}

	@Override
	public void softdel(final Connection connection, final TargetOperation operation) throws SQLException {
		StringBuffer sbSet = new StringBuffer();
		StringBuffer sbWhe = new StringBuffer();
		List<String> params = new ArrayList<String>();

		// gen sql
		// set
		for (Entry<String, String> e : operation.getDatMap().entrySet()) {
			if (sbSet.length() > 0) {
				sbSet.append(",");
			}
			sbSet.append(String.format("%s=default(%s)", e.getKey(), e.getKey()));
		}

		// where
		for (Entry<String, String> e : operation.getKeyMap().entrySet()) {
			if (sbWhe.length() == 0) {
				sbWhe.append("and ");
			}
			sbWhe.append(String.format("%s=?", e.getKey()));
			params.add(e.getValue());
		}

		String sql = String.format("update ignore %s set %s where 1=1 %s\n", operation.getTableName(), sbSet.toString(),
				sbWhe.toString());
//		System.out.println("UldraMysqlApplier=>softdel");
//		System.out.printf(">> " + sql);
		executeUpdate(connection, sql, params);
	}

	@Override
	public void begin(final Connection connection) throws SQLException {
//		connection.setAutoCommit(false);
		connection.prepareStatement("begin").execute();

	}

	@Override
	public void commit(final Connection connection) throws SQLException {
//		connection.commit();
		connection.prepareStatement("commit").execute();
	}

	@Override
	public void rollback(final Connection connection) throws SQLException {
//		connection.rollback();
		connection.prepareStatement("rollback").execute();
	}

	private static void executeUpdate(final Connection connection, final String sql, final List<String> params)
			throws SQLException {
		PreparedStatement pstmt = connection.prepareStatement(sql);
		int seq = 1;
		for (String param : params) {
			pstmt.setString(seq++, param);
		}
		pstmt.executeUpdate();
//		System.out.println(pstmt);
		pstmt.close();

	}

}
