package net.gywn.binlog.beans;

import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.gywn.binlog.api.TargetHandler;

public enum TargetOpType {
	INSERT {
		@Override
		public void executeUpdate(final BinlogTransaction binlogTransaction, final TargetOperation targetOperation,
				final TargetHandler targetHandler) throws Exception {

			// check data map if empty
			if (targetOperation.getDatMap().isEmpty()) {
				logger.debug("TargetOpType->INSERT->NO_DATA {}", targetOperation);
				return;
			}

			if (binlogTransaction.isRecovering()) {
				logger.debug("TargetOpType->INSERT->UPSERT");
				targetHandler.upsert(binlogTransaction.getConnection(), targetOperation);
				return;
			}
			logger.debug("TargetOpType->INSERT");
			targetHandler.insert(binlogTransaction.getConnection(), targetOperation);
		}
	},
	UPDATE {
		@Override
		public void executeUpdate(final BinlogTransaction binlogTransaction, final TargetOperation targetOperation,
				final TargetHandler targetHandler) throws Exception {

			// check data map if empty
			if (targetOperation.getDatMap().isEmpty()) {
				logger.debug("TargetOpType->INSERT->NO_DATA {}", targetOperation);
				return;
			}

			// check key map if empty
			if (targetOperation.getKeyMap().isEmpty()) {
				logger.debug("TargetOpType->INSERT->NO_ROWKEY {}", targetOperation);
				return;
			}

			logger.debug("TargetOpType->UPDATE");
			targetHandler.update(binlogTransaction.getConnection(), targetOperation);
		}
	},
	UPSERT {
		@Override
		public void executeUpdate(final BinlogTransaction binlogTransaction, final TargetOperation targetOperation,
				final TargetHandler targetHandler) throws Exception {

			// check data map if empty
			if (targetOperation.getDatMap().isEmpty()) {
				logger.debug("TargetOpType->INSERT->NO_DATA {}", targetOperation);
				return;
			}

			logger.debug("TargetOpType->UPSERT");
			targetHandler.upsert(binlogTransaction.getConnection(), targetOperation);
		}
	},
	DELETE {
		@Override
		public void executeUpdate(final BinlogTransaction binlogTransaction, final TargetOperation targetOperation,
				final TargetHandler targetHandler) throws Exception {

			// check key map if empty
			if (targetOperation.getKeyMap().isEmpty()) {
				logger.debug("TargetOpType->INSERT->NO_ROWKEY {}", targetOperation);
				return;
			}

			logger.debug("TargetOpType->DELETE");
			targetHandler.delete(binlogTransaction.getConnection(), targetOperation);
		}
	},
	SOFT_DELETE {
		@Override
		public void executeUpdate(final BinlogTransaction binlogTransaction, final TargetOperation targetOperation,
				final TargetHandler targetHandler) throws Exception {
			logger.debug("TargetOpType->SOFT_DELETE");
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

	private static final Logger logger = LoggerFactory.getLogger(TargetOpType.class);

	public abstract void executeUpdate(final BinlogTransaction binlogTransaction, final TargetOperation targetOperation,
			final TargetHandler targetHandler) throws Exception;

//	private static final String INSERT = "insert %s into %s (%s) values (%s)";
//	private static final String UPSERT = "insert ignore into %s (%s) values (%s) on duplicate key update %s";
//	private static final String UPDATE = "update ignore %s set %s where 1 = 1 %s";
//	private static final String DELETE = "delete from %s where 1 = 1 %s";
}
