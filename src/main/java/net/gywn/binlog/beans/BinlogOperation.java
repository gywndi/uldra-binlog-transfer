package net.gywn.binlog.beans;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.prometheus.client.Summary;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import net.gywn.binlog.common.UldraUtil;

@Getter
@ToString
public class BinlogOperation {
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
			binlogTable.getRowHandler().modify(this);
		}
		modified = true;
	}
}
