package net.gywn.binlog.api;

import java.sql.Connection;
import java.sql.SQLException;

import net.gywn.binlog.beans.TargetOperation;
import net.gywn.binlog.common.UldraConfig;

public interface TargetHandler {

	public void init(final UldraConfig uldraConfig) throws Exception;

	public void begin(final Connection connection) throws SQLException;

	public void commit(final Connection connection) throws SQLException;

	public void rollback(final Connection connection) throws SQLException;

	public void insert(final Connection connection, final TargetOperation operation) throws SQLException;

	public void upsert(final Connection connection, final TargetOperation operation) throws SQLException;

	public void update(final Connection connection, final TargetOperation operation) throws SQLException;

	public void delete(final Connection connection, final TargetOperation operation) throws SQLException;

	public void softdel(final Connection connection, final TargetOperation operation) throws SQLException;

}
