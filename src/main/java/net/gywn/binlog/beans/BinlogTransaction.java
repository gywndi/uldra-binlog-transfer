package net.gywn.binlog.beans;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@ToString
public class BinlogTransaction implements AutoCloseable {
	private final List<BinlogOperation> binlogOperations = new ArrayList<BinlogOperation>();
	private final String position;
	private final Binlog binlog;
	private boolean transactional = false;
	private boolean recovering = false;

	@Setter
	private Connection connection;

	public BinlogTransaction(final String position, final boolean recovering) {
		this.position = position;
		this.recovering = recovering;
		this.binlog = new Binlog(position);
	}

	public void addOperation(final BinlogOperation binlogOperation) {
		if (!transactional && binlogOperation.getBinlogTable().getTargetTables().size() > 1) {
			transactional = true;
		}
		binlogOperations.add(binlogOperation);
	}

	public int size() {
		return binlogOperations.size();
	}

	public boolean isTransactional() {
		if (transactional || binlogOperations.size() > 1) {
			return true;
		}
		return false;
	}

	public void close() {
		try {
			connection.close();
		} catch (Exception e) {
		}
	}
}
