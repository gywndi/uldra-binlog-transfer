package net.gywn.binlog.beans;

import java.sql.SQLException;
import net.gywn.binlog.api.TargetHandler;

public enum BinlogOpType {
	INS {
		@Override
		public void executeBinlogOperation(final BinlogTransaction binlogTransaction,
				final BinlogOperation binlogOperation, final TargetHandler targetHandler) throws Exception {
			for (final TargetTable targetTable : binlogOperation.getBinlogTable().getTargetTables()) {
				TargetOperation targetOperation = new TargetOperation(targetTable, binlogOperation);
				targetTable.getInsert().executeUpdate(binlogTransaction, targetOperation, targetHandler);
			}
		}

	},
	UPD {
		@Override
		public void executeBinlogOperation(final BinlogTransaction binlogTransaction,
				final BinlogOperation binlogOperation, final TargetHandler targetHandler) throws Exception {
			for (final TargetTable targetTable : binlogOperation.getBinlogTable().getTargetTables()) {
				TargetOperation targetOperation = new TargetOperation(targetTable, binlogOperation);
				targetTable.getUpdate().executeUpdate(binlogTransaction, targetOperation, targetHandler);
			}
		}
	},
	DEL {
		@Override
		public void executeBinlogOperation(final BinlogTransaction binlogTransaction,
				final BinlogOperation binlogOperation, final TargetHandler targetHandler) throws Exception {
			for (final TargetTable targetTable : binlogOperation.getBinlogTable().getTargetTables()) {
				TargetOperation targetOperation = new TargetOperation(targetTable, binlogOperation);
				targetTable.getDelete().executeUpdate(binlogTransaction, targetOperation, targetHandler);
			}
		}
	},
	NON {
		@Override
		public void executeBinlogOperation(final BinlogTransaction binlogTransaction,
				final BinlogOperation binlogOperation, final TargetHandler targetHandler) throws Exception {
		}
	};

	public abstract void executeBinlogOperation(final BinlogTransaction binlogTransaction,
			final BinlogOperation binlogOperation, final TargetHandler targetHandler) throws Exception;

}
