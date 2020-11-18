package net.gywn.binlog.api;

import net.gywn.binlog.beans.BinlogOperation;

public interface RowHandler {
	public void init();

	public void modify(final BinlogOperation operation);
}
