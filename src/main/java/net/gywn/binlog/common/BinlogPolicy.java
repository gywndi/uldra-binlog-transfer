package net.gywn.binlog.common;

import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import net.gywn.binlog.api.RowHandler;

@Setter
@Getter
@ToString
public class BinlogPolicy {
	private String name;
	private String partitionKey;
	private String rowHandlerClassName;
	private Map<String, String> rowHandlerParams = new HashMap<String, String>();
	private ReplicatPolicy[] replicatPolicies;
	private RowHandler rowHandler;
//	private boolean softDelete = false;

	public void setName(String name) {
		this.name = name.toLowerCase();
	}

	public void loadRowHandler() throws Exception {
		this.rowHandler = (RowHandler) (Class.forName(rowHandlerClassName)).newInstance();
		this.rowHandler.init();
	}
}
