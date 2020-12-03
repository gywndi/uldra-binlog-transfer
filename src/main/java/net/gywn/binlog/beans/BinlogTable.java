package net.gywn.binlog.beans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.ToString;
import net.gywn.binlog.api.RowHandler;
import net.gywn.binlog.common.BinlogPolicy;
import net.gywn.binlog.common.ReplicatPolicy;

@Getter
@ToString
public class BinlogTable {
	private static final Logger logger = LoggerFactory.getLogger(BinlogTable.class);

	private final String name;
	private final List<BinlogColumn> columns;
	private final List<BinlogColumn> rowKeys;
	private final List<TargetTable> targetTables = new ArrayList<TargetTable>();
	private final Map<String, String> partitionKeyMap = new HashMap<String, String>();
	private boolean target = true;
	private BinlogPolicy binlogPolicy;
	private RowHandler rowHandler;

	public BinlogTable(final String name, List<BinlogColumn> columns, List<BinlogColumn> rowKeys,
			final BinlogPolicy binlogPolicy) {
		this.name = name;
		this.columns = columns;
		this.rowKeys = rowKeys;
		this.binlogPolicy = binlogPolicy;

		if (binlogPolicy == null) {
			logger.debug("{} is not target", name);
			this.target = false;
			return;
		}

		// ===========================================
		// Check partition key for parallel processing
		// ===========================================
		if (binlogPolicy.getPartitionKey() != null) {
			for (BinlogColumn column : rowKeys) {
				if (column.getName().equalsIgnoreCase(binlogPolicy.getPartitionKey())) {
					partitionKeyMap.put(column.getName(), column.getName());
				}
			}
		}

		// partition key not defined
		if (partitionKeyMap.isEmpty()) {
			logger.info("Partition key `{}` must be inside primary key", binlogPolicy.getPartitionKey(), rowKeys);
			logger.info("Set partition key to primary key - {}", rowKeys);
			for (BinlogColumn column : rowKeys) {
				partitionKeyMap.put(column.getName(), column.getName());
			}
		}

		// ===========================================
		// Set replication policy in binlog table
		// ===========================================
		for (ReplicatPolicy replicatPolicy : binlogPolicy.getReplicatPolicies()) {
			TargetTable targetTable = new TargetTable(this.columns, replicatPolicy);
			targetTables.add(targetTable);
		}

		// ===========================================
		// Set row handler in binlog table
		// ===========================================
		this.rowHandler = binlogPolicy.getRowHandler();

	}

	public boolean isTarget() {
		logger.debug("`{}` target is {}", this.name, this.target);
		return target;
	}
}
