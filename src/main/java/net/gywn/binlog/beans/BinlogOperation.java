package net.gywn.binlog.beans;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@ToString
public class BinlogOperation {
	private static final Logger logger = LoggerFactory.getLogger(BinlogOperation.class);
	private final BinlogTable binlogTable;
	private final BinlogOpType binlogOpType;
	private final Map<String, String> datMap = new HashMap<String, String>();
	private final Map<String, String> keyMap = new HashMap<String, String>();
	private boolean modified = false;

	@Setter
	private long crc32Code;

	public BinlogOperation(final BinlogTable binlogTable, final BinlogOpType binlogOpType) {
		this.binlogTable = binlogTable;
		this.binlogOpType = binlogOpType;
	}

	public void modify() {
		if (!modified) {
			logger.debug("modify->before {}", this);
			binlogTable.getRowHandler().modify(this);
			logger.debug("modify->after  {}", this);
		}
		modified = true;
	}
}
