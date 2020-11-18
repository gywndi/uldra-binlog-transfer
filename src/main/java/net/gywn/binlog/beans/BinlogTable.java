package net.gywn.binlog.beans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import net.gywn.binlog.api.RowHandler;
import net.gywn.binlog.common.BinlogPolicy;
import net.gywn.binlog.common.ReplicatPolicy;

@Getter
@ToString
public class BinlogTable {
	private final String name;
	private final List<BinlogColumn> columns;
	private final List<BinlogColumn> rowKeys;
	private final List<TargetTable> targetTables = new ArrayList<TargetTable>();
	private final Map<String, String> partitionKeyMap = new HashMap<String, String>();
	private BinlogPolicy binlogPolicy;
	private RowHandler rowHandler;
//	private BinlogOpType INS = BinlogOpType.INS;
//	private BinlogOpType UPD = BinlogOpType.UPD;
//	private BinlogOpType DEL = BinlogOpType.DEL;

//	public BinlogTable(final String name, final BinlogPolicy binlogPolicy) {
//		this.name = name;
//		this.binlogPolicy = binlogPolicy;
//
//		if (binlogPolicy != null) {
//			this.rowHandler = binlogPolicy.getRowHandler();
//
//			if (binlogPolicy.isWipingDelete()) {
//				this.DEL = BinlogOpType.NIL;
//			}
//		}
//	}

	public BinlogTable(final String name, List<BinlogColumn> columns, List<BinlogColumn> rowKeys,
			final BinlogPolicy binlogPolicy) {
		this.name = name;
		this.columns = columns;
		this.rowKeys = rowKeys;
		this.binlogPolicy = binlogPolicy;

		if (binlogPolicy != null) {
			this.rowHandler = binlogPolicy.getRowHandler();

			// ================================
			// 파티셔닝 키가 정의된 경우, MySQL 칼럼에 있어야함
			// ================================
			if (binlogPolicy.getPartitionKey() != null) {
				for (BinlogColumn column : rowKeys) {
//					System.out.println("@@@" + column.getName() + "===>" + binlogPolicy.getPartitionKey());
					if (column.getName().equalsIgnoreCase(binlogPolicy.getPartitionKey())) {
						partitionKeyMap.put(column.getName(), column.getName());
					}
				}
			}

			// ================================
			// 파티셔닝 키가 정의안됐거나, PK에 없는 경우
			// ================================
			if (partitionKeyMap.isEmpty()) {
				System.out.println(
						"Partition key is not defined or not in primary key, set partition map to primary key");
				for (BinlogColumn column : rowKeys) {
					partitionKeyMap.put(column.getName(), column.getName());
				}
			}
			for (ReplicatPolicy replicatPolicy : binlogPolicy.getReplicatPolicies()) {
				TargetTable targetTable = new TargetTable(this.columns, replicatPolicy);
				targetTables.add(targetTable);
			}
		}

	}

	public boolean isTarget() {
		return binlogPolicy != null;
	}

}
