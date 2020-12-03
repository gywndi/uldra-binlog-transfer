package net.gywn.binlog;

import static com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY;
import static com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG_MICRO;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.BinaryLogClient.LifecycleListener;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import lombok.Data;
import lombok.Getter;
import net.gywn.binlog.beans.Binlog;
import net.gywn.binlog.beans.BinlogColumn;
import net.gywn.binlog.beans.BinlogOpType;
import net.gywn.binlog.beans.BinlogOperation;
import net.gywn.binlog.beans.BinlogTable;
import net.gywn.binlog.beans.BinlogTransaction;
import net.gywn.binlog.common.BinlogPolicy;
import net.gywn.binlog.common.UldraConfig;
import net.gywn.binlog.common.UldraUtil;

@Data
public class BinlogEventHandler {
	private static final Logger logger = LoggerFactory.getLogger(BinlogEventHandler.class);

	private String binlogServer;
	private int binlogServerID;
	private String binlogServerUsername;
	private String binlogServerPassword;
	private String binlogInfoFile;
	private UldraConfig uldraConfig;

	private BasicDataSource binlogDataSource = new BasicDataSource();
	private BinaryLogClient binaryLogClient = null;
	private BinlogEventWorker[] binlogEventWorkers;
	private final Map<Integer, BinlogTransaction> miniTransactions = new HashMap<Integer, BinlogTransaction>();

	@Getter
	private Binlog currntBinlog;
	private Binlog targetBinlog;
	private Long workingBinlogPosition = 0L;
	private Long lastBinlogFlushTimeMillis = 0L;

	@Getter
	private Map<Long, BinlogTable> binlogTableMap = new HashMap<Long, BinlogTable>();

	@Getter
	private Calendar time = Calendar.getInstance();

	private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	@Getter
	private boolean threadRunning = false;
	@Getter
	private Exception threadException;

	private boolean recovering = true;

	private BinlogTransaction binlogTransaction = null;
	private boolean isPartitionKeyChanged = false;

	public BinlogEventHandler(final UldraConfig uldraConfig) {
		this.uldraConfig = uldraConfig;
		this.binlogServer = uldraConfig.getBinlogServer();
		this.binlogServerID = uldraConfig.getBinlogServerID();
		this.binlogServerUsername = uldraConfig.getBinlogServerUsername();
		this.binlogServerPassword = uldraConfig.getBinlogServerPassword();
		this.binlogInfoFile = uldraConfig.getBinlogInfoFile();
		this.binlogServerPassword = this.binlogServerPassword == null ? "" : this.binlogServerPassword;
	}

	public void start() {
		try {
			// ===================================
			// DataSource initialize
			// ===================================
			Class.forName("com.mysql.jdbc.Driver");
			String jdbcUrl = String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false&connectTimeout=3000",
					this.binlogServer, "information_schema");
			binlogDataSource.setUrl(jdbcUrl);
			binlogDataSource.setUsername(binlogServerUsername);
			binlogDataSource.setPassword(binlogServerPassword);
			binlogDataSource.setDefaultAutoCommit(true);
			binlogDataSource.setInitialSize(5);
			binlogDataSource.setMinIdle(5);
			binlogDataSource.setMaxIdle(5);
			binlogDataSource.setMaxTotal(10);
			binlogDataSource.setMaxWaitMillis(1000);
			binlogDataSource.setTestOnBorrow(false);
			binlogDataSource.setTestOnReturn(false);
			binlogDataSource.setTestWhileIdle(true);
			binlogDataSource.setNumTestsPerEvictionRun(3);
			binlogDataSource.setTimeBetweenEvictionRunsMillis(60000);
			binlogDataSource.setMinEvictableIdleTimeMillis(600000);
			binlogDataSource.setValidationQuery("SELECT 1");
			binlogDataSource.setValidationQueryTimeout(5);

			// Initialize binlog client
			String[] binlogServerInfo = binlogServer.split(":");
			String binlogServerUrl = binlogServerInfo[0];
			int binlogServerPort = 3306;
			try {
				binlogServerPort = Integer.parseInt(binlogServerInfo[1]);
			} catch (Exception e) {
				logger.info("Binlog server port not defined, set {}", binlogServerPort);
			}

			binaryLogClient = new BinaryLogClient(binlogServerUrl, binlogServerPort, binlogServerUsername,
					binlogServerPassword);
			EventDeserializer eventDeserializer = new EventDeserializer();
			eventDeserializer.setCompatibilityMode(DATE_AND_TIME_AS_LONG_MICRO, CHAR_AND_BINARY_AS_BYTE_ARRAY);
			binaryLogClient.setEventDeserializer(eventDeserializer);
			binaryLogClient.setServerId(binlogServerID);
			registerEventListener();
			registerLifecycleListener();

			Binlog currentServerBinlog = getCurrentBinlog();

			// ============================
			// load binlog position
			// ============================
			try {
				Binlog[] binlogs = Binlog.read(binlogInfoFile);
				currntBinlog = binlogs[0];
				targetBinlog = binlogs[1];
			} catch (Exception e) {
				logger.info("Start binlog position from {}", currentServerBinlog);
				currntBinlog = currentServerBinlog;
			}

			if (targetBinlog == null) {
				logger.info("Start binlog position from {}", currentServerBinlog);
				targetBinlog = currentServerBinlog;
			}

			logger.info("Replicator start from {} to {}", currntBinlog, targetBinlog);
			binaryLogClient.setBinlogFilename(currntBinlog.getBinlogFile());
			binaryLogClient.setBinlogPosition(currntBinlog.getBinlogPosition());

//			binaryLogClient.setBinlogFilename("binlog.000001");
//			binaryLogClient.setBinlogPosition(155);

			// =========================
			// Create transaction worker
			// =========================
			binlogEventWorkers = new BinlogEventWorker[uldraConfig.getWorkerCount()];
			for (int i = 0; i < binlogEventWorkers.length; i++) {
				try {
					binlogEventWorkers[i] = new BinlogEventWorker(i, uldraConfig);
					binlogEventWorkers[i].start();
				} catch (Exception e) {
					logger.error("Start binlog event worker[{}] failed - {}", i, e.getMessage());
					System.exit(1);
				}
			}

			// ========================================
			// binlog flush (every 500ms) & monitoring
			// ========================================
			new Thread(new Runnable() {

				public void run() {
					while (true) {
						List<Binlog> binlogList = new ArrayList<Binlog>();
						for (BinlogEventWorker mysqlTransactionWorker : binlogEventWorkers) {
							Binlog binlog = mysqlTransactionWorker.getLastExecutedBinlog();
							if (binlog != null) {
								binlogList.add(binlog);
							}
						}
						
						int currentJobCount = getJobCount();
						Binlog binlog = null, lastBinlog = null;
						if (binlogList.size() > 0) {
							Binlog[] binlogArray = new Binlog[binlogList.size()];
							binlogList.toArray(binlogArray);
							Arrays.sort(binlogArray);
							binlog = currentJobCount > 0 ? binlogArray[0]:binlogArray[binlogArray.length-1];
							lastBinlog = currntBinlog;
						}

						if (binlog == null) {
							binlog = currntBinlog;
							lastBinlog = targetBinlog;
						}

						if (recovering && !isRecoveringPosition()) {
							logger.info("Recover finished, target - {}", targetBinlog);
							recovering = false;
						}

						// flush binlog position info
						try {
							Binlog.flush(binlog, lastBinlog, binlogInfoFile);
						} catch (IOException e) {
							logger.debug("Flush failed - ", e);
						}

						UldraUtil.sleep(500);
					}
				}

			}, "uldra").start();
			binaryLogClient.connect();

		} catch (Exception e) {
			threadException = e;
			threadRunning = false;
			logger.error(e.toString());
		}

	}

	private void registerEventListener() {
		final BinlogEventHandler binlogEventHandler = this;
		binaryLogClient.registerEventListener(new EventListener() {
			public void onEvent(Event event) {
				EventType eventType = event.getHeader().getEventType();
				BinlogEvent.valuOf(eventType).receiveEvent(event, binlogEventHandler);
			}
		});
	}

	private void registerLifecycleListener() {
		binaryLogClient.registerLifecycleListener(new LifecycleListener() {

			public void onConnect(BinaryLogClient client) {
				logger.info("mysql-binlog-connector start from {}:{}", client.getBinlogFilename(),
						client.getBinlogPosition());
				threadRunning = true;
			}

			public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
				logger.error("CommunicationFailure - {}", ex.getMessage());
				threadRunning = false;
			}

			public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
				logger.error("EventDeserializationFailur - {}", ex.getMessage());
				threadRunning = false;
			}

			public void onDisconnect(BinaryLogClient client) {
				logger.info("Disconnect");
				threadRunning = false;
			}
		});

	}

	public void receiveWriteRowsEvent(final Event event) {
		WriteRowsEventData eventData = (WriteRowsEventData) event.getData();
		BinlogTable binlogTable = binlogTableMap.get(eventData.getTableId());

		String keyStr = "";
		if (!binlogTable.isTarget()) {
			return;
		}

		BitSet bit = eventData.getIncludedColumns();

		logger.debug("Binlog operation counts: {}", eventData.getRows().size());
		for (Serializable[] row : eventData.getRows()) {
			BinlogOperation binlogOperation = new BinlogOperation(binlogTable, BinlogOpType.INS);

			// =====================
			// New image
			// =====================
			int seq = -1;
			for (Serializable serializable : row) {
				seq = bit.nextSetBit(seq + 1);
				BinlogColumn column = binlogTable.getColumns().get(seq);
				if (column != null) {
					String key = column.getName();
					String value = UldraUtil.toString(serializable, column);
					binlogOperation.getDatMap().put(key, value);
				}
			}

			for (BinlogColumn column : binlogTable.getRowKeys()) {
				String key = column.getName();
				String value = binlogOperation.getDatMap().get(key);
				if (binlogTable.getPartitionKeyMap().containsKey(key)) {
					keyStr += value;
				}
				binlogOperation.getKeyMap().put(key, value);
			}

			binlogOperation.setCrc32Code(UldraUtil.crc32(keyStr));
			logger.debug("binlogOperation - {}", binlogOperation);

			binlogTransaction.addOperation(binlogOperation);
		}
	}

	public void receiveUpdateRowsEvent(final Event event) {
		UpdateRowsEventData eventData = (UpdateRowsEventData) event.getData();
		BinlogTable binlogTable = binlogTableMap.get(eventData.getTableId());

		String keyStr = "";
		if (!binlogTable.isTarget()) {
			return;
		}

		int seq;
		BitSet oldBit = eventData.getIncludedColumnsBeforeUpdate();
		BitSet newBit = eventData.getIncludedColumns();

		logger.debug("Binlog operation counts: {}", eventData.getRows().size());
		for (Entry<Serializable[], Serializable[]> entry : eventData.getRows()) {
			BinlogOperation binlogOperation = new BinlogOperation(binlogTable, BinlogOpType.UPD);

			// =====================
			// New image
			// =====================
			seq = -1;
			for (Serializable serializable : entry.getValue()) {
				seq = newBit.nextSetBit(seq + 1);
				BinlogColumn column = binlogTable.getColumns().get(seq);
				if (column != null) {
					String key = column.getName();
					String value = UldraUtil.toString(serializable, column);
					binlogOperation.getDatMap().put(key, value);
				}
			}

			// =====================
			// Old image
			// =====================
			seq = -1;
			for (Serializable serializable : entry.getKey()) {
				seq = oldBit.nextSetBit(seq + 1);
				BinlogColumn column = binlogTable.getColumns().get(seq);
				if (column != null && column.isRowKey()) {
					String key = column.getName();
					String value = UldraUtil.toString(serializable, column);
					if (binlogTable.getPartitionKeyMap().containsKey(key)) {
						if (binlogOperation.getDatMap().containsKey(key)) {
							String afterValue = binlogOperation.getDatMap().get(key);
							if (afterValue != null && !afterValue.equals(value)) {
								logger.debug("Partition key changed, `{}`->`{}`", value, afterValue);
								isPartitionKeyChanged = true;
							}
						}
						keyStr += value;
					}
					binlogOperation.getKeyMap().put(key, value);
				}
			}

			binlogOperation.setCrc32Code(UldraUtil.crc32(keyStr));
			logger.debug("binlogOperation - {}", binlogOperation);

			binlogTransaction.addOperation(binlogOperation);

		}
	}

	public void receiveDeleteRowsEvent(final Event event) {
		DeleteRowsEventData eventData = (DeleteRowsEventData) event.getData();
		BinlogTable binlogTable = binlogTableMap.get(eventData.getTableId());

		String keyStr = "";
		if (!binlogTable.isTarget()) {
			return;
		}

		BitSet bit = eventData.getIncludedColumns();

		logger.debug("Binlog operation counts: {}", eventData.getRows().size());
		for (Serializable[] row : eventData.getRows()) {
			BinlogOperation binlogOperation = new BinlogOperation(binlogTable, BinlogOpType.DEL);

			// =====================
			// Delete image
			// =====================
			int seq = -1;
			for (Serializable serializable : row) {
				seq = bit.nextSetBit(seq + 1);
				BinlogColumn column = binlogTable.getColumns().get(seq);
				if (column != null && column.isRowKey()) {
					String key = column.getName();
					String value = UldraUtil.toString(serializable, column);
					if (binlogTable.getPartitionKeyMap().containsKey(key)) {
						keyStr += value;
					}
					binlogOperation.getKeyMap().put(key, value);
				}
			}

			binlogOperation.setCrc32Code(UldraUtil.crc32(keyStr));
			logger.debug("binlogOperation - {}", binlogOperation);

			binlogTransaction.addOperation(binlogOperation);
		}
	}

	public void receiveTableMapEvent(final Event event) {
		TableMapEventData eventData = (TableMapEventData) event.getData();

		if (binlogTableMap.containsKey(eventData.getTableId())) {
			return;
		}

		logger.info("Meta info for TABLE_ID_{} is not in cache (`{}`.`{}`)", eventData.getTableId(),
				eventData.getDatabase(), eventData.getTable());
		binlogTableMap.put(eventData.getTableId(), getBinlogTable(eventData));
		BinlogTable binlogTable = binlogTableMap.get(eventData.getTableId());

		if (!binlogTable.isTarget()) {
			logger.info("Skip `{}`.`{}`, not target", eventData.getDatabase(), eventData.getTable());
			return;
		}

		logger.info("Remove legacy meta info for `{}`.`{}`", eventData.getDatabase(), eventData.getTable());
		for (Entry<Long, BinlogTable> entry : binlogTableMap.entrySet()) {
			if (entry.getKey() != eventData.getTableId() && entry.getValue().getName().equals(binlogTable.getName())) {
				binlogTableMap.remove(entry.getKey());
				break;
			}
		}
	}

	public void receiveRotateEvent(final Event event) {
		RotateEventData eventData = (RotateEventData) event.getData();
		currntBinlog.setBinlogFile(eventData.getBinlogFilename());
		currntBinlog.setBinlogPosition(eventData.getBinlogPosition());
		logger.info("Binlog rotated - {}", currntBinlog);
	}

	public void receiveQueryEvent(final Event event) {
		EventHeaderV4 header = event.getHeader();
		currntBinlog.setBinlogPosition(header.getPosition());

		QueryEventData eventData = (QueryEventData) event.getData();
		switch (eventData.getSql()) {
		case "BEGIN":
			transactionStart();
			break;
		case "COMMIT":
			transactionEnd();
			break;
		default:
			logger.debug(event.toString());
		}
	}

	public void receiveXidEvent(final Event event) {
		try {
			EventHeaderV4 header = event.getHeader();
			transactionEnd();
		} finally {
		}
	}

	private void transactionStart() {
		logger.debug("transactionStart =====>");

		if (binlogTransaction == null) {
			logger.debug("create binlogTransaction");
			binlogTransaction = new BinlogTransaction(currntBinlog.toString(), recovering);
		}
	}

	private void transactionEnd() {
		logger.debug("transactionEnd");

		try {

			// =======================================
			// Empty transaction
			// =======================================
			if (binlogTransaction.size() == 0) {
				logger.debug("No operation");
				return;
			}

			// =======================================
			// single operation
			// =======================================
			if (binlogTransaction.size() == 1) {
				BinlogOperation binlogOperation = binlogTransaction.getBinlogOperations().get(0);
				int slot = (int) (binlogOperation.getCrc32Code() % uldraConfig.getWorkerCount());

				logger.debug("Single operation, slot {} - {}", slot, binlogOperation);
				binlogEventWorkers[slot].enqueue(binlogTransaction);
				return;
			}

			// =======================================
			// partiton key has been changed
			// =======================================
			if (isPartitionKeyChanged) {
				logger.debug("Partition key changed, single transaction processiong");
				waitJobProcessing();
				binlogEventWorkers[0].enqueue(binlogTransaction);
				waitJobProcessing();
				return;
			}

			// =======================================
			// transction -> mini-trx by partition key
			// =======================================
			miniTransactions.clear();
			for (final BinlogOperation binlogOperation : binlogTransaction.getBinlogOperations()) {
				logger.debug("Partition key changed");
				int slot = (int) (binlogOperation.getCrc32Code() % uldraConfig.getWorkerCount());

				// new mini trx if not exists in trx map
				if (!miniTransactions.containsKey(slot)) {
					logger.debug("Create new mini-trx slot: {}", slot);
					miniTransactions.put(slot, new BinlogTransaction(binlogTransaction.getPosition(), recovering));
				}

				// add operation in mini trx
				logger.debug("Add operation in mini-trx slot {} - {}", slot, binlogOperation);
				miniTransactions.get(slot).addOperation(binlogOperation);
			}

			// ======================================
			// enqueue mini transactions
			// ======================================
			for (final Entry<Integer, BinlogTransaction> entry : miniTransactions.entrySet()) {
				logger.debug("Enqueue mini-trx {}", entry.getKey());
				binlogEventWorkers[entry.getKey()].enqueue(entry.getValue());
			}
		} finally {
			isPartitionKeyChanged = false;
			binlogTransaction = null;
		}
	}

	private boolean isRecoveringPosition() {
		if (targetBinlog.compareTo(currntBinlog) > 0) {
			logger.debug("Recovering position [current]{} [target]{}", currntBinlog, targetBinlog);
			return true;
		}
		logger.debug("Recovering mode false");

		return false;
	}

	private Binlog getCurrentBinlog() throws SQLException {
		Connection connection = null;
		Binlog binlogServerBinlog = null;
		try {
			connection = binlogDataSource.getConnection();
			String query = "show master status";
			PreparedStatement pstmt = connection.prepareStatement(query);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				binlogServerBinlog = new Binlog(rs.getString("File"), rs.getLong("Position"));
				return binlogServerBinlog;
			}
			pstmt.close();
		} finally {
			logger.debug("Current binlog position from binlog server: {}", binlogServerBinlog);
			try {
				connection.close();
			} catch (Exception e) {
			}
		}
		return null;
	}

	private BinlogTable getBinlogTable(final TableMapEventData tableMapEventData) {
		logger.debug("Get meta info from database for {}", tableMapEventData);

		// Binlog policy
		String database = tableMapEventData.getDatabase().toLowerCase();
		String table = tableMapEventData.getTable().toLowerCase();
		String name = String.format("%s.%s", database, table);
		BinlogPolicy binlogPolicy = uldraConfig.getBinlogPolicyMap().get(name);

		Connection connection = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String query = null;
		while (true) {
			try {

				// Get connection
				connection = binlogDataSource.getConnection();

				// Get columns
				List<BinlogColumn> columns = new ArrayList<BinlogColumn>();
				query = " select ordinal_position,";
				query += "  lower(column_name) column_name,";
				query += "  lower(character_set_name) character_set_name,";
				query += "  lower(data_type) data_type,";
				query += "  instr(column_type, 'unsigned') is_unsigned";
				query += " from information_schema.columns";
				query += " where table_schema = ?";
				query += " and table_name = ?";
				query += " order by ordinal_position";

				pstmt = connection.prepareStatement(query);
				pstmt.setString(1, database);
				pstmt.setString(2, table);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					String columnName = rs.getString("column_name").toLowerCase();
					String columnCharset = rs.getString("character_set_name");
					String dataType = rs.getString("data_type");
					boolean columnUnsigned = rs.getBoolean("is_unsigned");
					columns.add(new BinlogColumn(columnName, dataType, columnCharset, columnUnsigned));
				}
				rs.close();
				pstmt.close();

				// Get primary key & unique key
				List<BinlogColumn> rowKeys = new ArrayList<BinlogColumn>();
				query = " select distinct ";
				query += "   column_name ";
				query += " from information_schema.table_constraints a ";
				query += " inner join information_schema.statistics b on b.table_schema = a.table_schema ";
				query += "   and a.table_name = b.table_name ";
				query += "   and b.index_name = a.constraint_name ";
				query += " where lower(a.constraint_type) in ('primary key') ";
				query += " and a.table_schema = ? ";
				query += " and a.table_name = ? ";

				pstmt = connection.prepareStatement(query);
				pstmt.setString(1, database);
				pstmt.setString(2, table);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					String columnName = rs.getString("column_name").toLowerCase();
					for (BinlogColumn column : columns) {
						if (column.getName().equals(columnName)) {
							column.setRowKey(true);
							rowKeys.add(column);
							break;
						}
					}
				}
				rs.close();
				pstmt.close();

				return new BinlogTable(name, columns, rowKeys, binlogPolicy);

			} catch (Exception e) {
				logger.error(e.getMessage());
				UldraUtil.sleep(1000);
			} finally {
				try {
					connection.close();
				} catch (SQLException e) {
					logger.error(e.getMessage());
				}
			}
		}
	}

	// wait queue processing
	private void waitJobProcessing() {
		int sleepMS = 1;
		while (true) {

			if (getJobCount() == 0) {
				break;
			}

			logger.debug("Sleep {}ms", sleepMS);
			UldraUtil.sleep(sleepMS);
			sleepMS *= 2;
		}
	}

	private int getJobCount() {
		int currentJobs = 0;
		for (BinlogEventWorker worker : binlogEventWorkers) {
			currentJobs += worker.getJobCount();
		}

		logger.debug("Current remain jobs {}", currentJobs);
		return currentJobs;
	}

	private enum BinlogEvent {

		WRITE_ROWS {
			@Override
			public void receiveEvent(final Event event, final BinlogEventHandler binlogEventHandler) {
				binlogEventHandler.receiveWriteRowsEvent(event);
			}
		},
		UPDATE_ROWS {
			@Override
			public void receiveEvent(final Event event, final BinlogEventHandler binlogEventHandler) {
				binlogEventHandler.receiveUpdateRowsEvent(event);
			}
		},
		DELETE_ROWS {
			@Override
			public void receiveEvent(final Event event, final BinlogEventHandler binlogEventHandler) {
				binlogEventHandler.receiveDeleteRowsEvent(event);
			}
		},
		TABLE_MAP {
			@Override
			public void receiveEvent(final Event event, final BinlogEventHandler binlogEventHandler) {
				binlogEventHandler.receiveTableMapEvent(event);
			}
		},
		ROTATE {
			@Override
			public void receiveEvent(final Event event, final BinlogEventHandler binlogEventHandler) {
				binlogEventHandler.receiveRotateEvent(event);
			}
		},
		QUERY {
			@Override
			public void receiveEvent(final Event event, final BinlogEventHandler binlogEventHandler) {
				binlogEventHandler.receiveQueryEvent(event);
			}
		},
		XID {
			@Override
			public void receiveEvent(final Event event, final BinlogEventHandler binlogEventHandler) {
				binlogEventHandler.receiveXidEvent(event);
			}
		},
		NOOP {
			@Override
			public void receiveEvent(final Event event, final BinlogEventHandler binlogEventHandler) {
			}
		};

		private static final Map<EventType, BinlogEvent> map = new HashMap<EventType, BinlogEvent>();
		static {
			// ==========================
			// Initialize
			// ==========================
			for (EventType e : EventType.values()) {
				map.put(e, NOOP);
			}

			// ==========================
			// Set Event Type Map
			// ==========================
			map.put(EventType.PRE_GA_WRITE_ROWS, WRITE_ROWS);
			map.put(EventType.WRITE_ROWS, WRITE_ROWS);
			map.put(EventType.EXT_WRITE_ROWS, WRITE_ROWS);

			map.put(EventType.PRE_GA_UPDATE_ROWS, UPDATE_ROWS);
			map.put(EventType.UPDATE_ROWS, UPDATE_ROWS);
			map.put(EventType.EXT_UPDATE_ROWS, UPDATE_ROWS);

			map.put(EventType.PRE_GA_DELETE_ROWS, DELETE_ROWS);
			map.put(EventType.DELETE_ROWS, DELETE_ROWS);
			map.put(EventType.EXT_DELETE_ROWS, DELETE_ROWS);

			map.put(EventType.TABLE_MAP, TABLE_MAP);
			map.put(EventType.QUERY, QUERY);
			map.put(EventType.ROTATE, ROTATE);
			map.put(EventType.XID, XID);

		}

		public abstract void receiveEvent(final Event event, final BinlogEventHandler binlogEventHandler);

		public static BinlogEvent valuOf(EventType eventType) {
			BinlogEvent binlogEvent = map.get(eventType);
			logger.debug("=====================================");
			logger.debug("{}->BinlogEvent.{}", eventType, binlogEvent);
			return binlogEvent;
		}

	}
}