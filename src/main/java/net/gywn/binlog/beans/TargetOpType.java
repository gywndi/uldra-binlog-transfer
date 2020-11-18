package net.gywn.binlog.beans;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.DataSource;

import net.gywn.binlog.api.TargetHandler;

public enum TargetOpType {
	INSERT {
		@Override
		public void executeUpdate(final BinlogTransaction binlogTransaction, final TargetOperation targetOperation,
				final TargetHandler targetHandler) throws SQLException {
			if (binlogTransaction.isRecovering()) {
				targetHandler.upsert(binlogTransaction.getConnection(), targetOperation);
				return;
			}
			targetHandler.insert(binlogTransaction.getConnection(), targetOperation);
		}
	},
	UPDATE {
		@Override
		public void executeUpdate(final BinlogTransaction binlogTransaction, final TargetOperation targetOperation,
				final TargetHandler targetHandler) throws SQLException {
			targetHandler.update(binlogTransaction.getConnection(), targetOperation);
		}
	},
	UPSERT {
		@Override
		public void executeUpdate(final BinlogTransaction binlogTransaction, final TargetOperation targetOperation,
				final TargetHandler targetHandler) throws SQLException {
			targetHandler.upsert(binlogTransaction.getConnection(), targetOperation);
		}
	},
	DELETE {
		@Override
		public void executeUpdate(final BinlogTransaction binlogTransaction, final TargetOperation targetOperation,
				final TargetHandler targetHandler) throws SQLException {
			targetHandler.delete(binlogTransaction.getConnection(), targetOperation);
		}
	},
	SOFT_DELETE {
		@Override
		public void executeUpdate(final BinlogTransaction binlogTransaction, final TargetOperation targetOperation,
				final TargetHandler targetHandler) throws SQLException {
			Map<String, String> keyMap = targetOperation.getKeyMap();
			Map<String, String> datMap = targetOperation.getDatMap();
			for (Entry<String, String> entry : targetOperation.getTargetTable().getColumnMapper().entrySet()) {
				String column = entry.getValue();
				if (keyMap.containsKey(column)) {
					continue;
				}
				datMap.put(column, null);
			}
			targetHandler.softdel(binlogTransaction.getConnection(), targetOperation);
		}
	};

	public abstract void executeUpdate(final BinlogTransaction binlogTransaction, final TargetOperation targetOperation,
			final TargetHandler targetHandler) throws SQLException;

//	private static final String INSERT = "insert %s into %s (%s) values (%s)";
//	private static final String UPSERT = "insert ignore into %s (%s) values (%s) on duplicate key update %s";
//	private static final String UPDATE = "update ignore %s set %s where 1 = 1 %s";
//	private static final String DELETE = "delete from %s where 1 = 1 %s";
}
