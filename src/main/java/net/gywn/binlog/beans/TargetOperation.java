package net.gywn.binlog.beans;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class TargetOperation {
	private final String tableName;
	private final Map<String, String> datMap;
	private final Map<String, String> keyMap;
	private final TargetTable targetTable;

	public TargetOperation(final TargetTable targetTable, final BinlogOperation binlogOperation) {
		this.targetTable = targetTable;
		this.tableName = targetTable.getName();
		this.datMap = targetTable.getTargetDataMap(binlogOperation.getDatMap());
		this.keyMap = targetTable.getTargetDataMap(binlogOperation.getKeyMap());
	}
}
