package net.gywn.binlog.beans;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;

import lombok.Getter;
import lombok.ToString;
import net.gywn.binlog.common.ReplicatPolicy;

@Getter
@ToString
public class TargetTable {
	private final String name;
	private final Map<String, String> columnMapper = new HashMap<String, String>();
	private final Map<String, String> loopupColumnMapper = new HashMap<String, String>();
	private boolean lookupMetaInitailized = false;

	private TargetOpType insert = TargetOpType.INSERT;
	private TargetOpType update = TargetOpType.UPDATE;
	private TargetOpType delete = TargetOpType.DELETE;

	public TargetTable(final List<BinlogColumn> binlogColumns, final ReplicatPolicy replicatPolicy) {

		this.name = replicatPolicy.getName();
		List<String> columns = replicatPolicy.getColums();

		// 타겟 칼럼이 지정안되었을 때, MySQL 칼럼 리스트 그대로 타겟을 지정하도록 세팅
		if (columns == null) {
			columns = new ArrayList<String>();
			for (BinlogColumn column : binlogColumns) {
				columns.add(column.getName());
			}
			replicatPolicy.setColums(columns);
		}
//		System.out.println(columns);

		// column map - ORIGIN_NAME:RENAME_NAME
		for (String column : columns) {
			String[] columnSplit = column.toLowerCase().split(":");
			columnMapper.put(columnSplit[0], columnSplit.length > 1 ? columnSplit[1] : columnSplit[0]);
		}

		// change upsert
		if (replicatPolicy.isUpsertMode()) {
			System.out.println(">>" + replicatPolicy.getName() + " set upsert mode");
			insert = TargetOpType.UPSERT;
			update = TargetOpType.UPSERT;
		}

		// change soft delete
		if (replicatPolicy.isSoftDelete()) {
			System.out.println(">>" + replicatPolicy.getName() + " set soft delete mode");
			delete = TargetOpType.SOFT_DELETE;
		}
	}

	public Map<String, String> getTargetMap(final Map<String, String> map) throws SQLException {
		Map<String, String> resultMap = new HashMap<String, String>();

		// copy binlog data target columns only
		for (Entry<String, String> entry : columnMapper.entrySet()) {
			String column = entry.getKey();
			String columnNew = entry.getKey();
			if (map.containsKey(entry.getKey())) {
				resultMap.put(columnNew, map.get(column));
			}
		}

		return resultMap;
	}

	public Map<String, String> getTargetDataMap(final Map<String, String> map) {
		Map<String, String> resultMap = new HashMap<String, String>();

		// copy binlog data target columns only
		for (Entry<String, String> entry : columnMapper.entrySet()) {
			String column = entry.getKey();
			String columnNew = entry.getValue();
			if (map.containsKey(entry.getKey())) {
				resultMap.put(columnNew, map.get(column));
			}
		}

		return resultMap;
	}
}
