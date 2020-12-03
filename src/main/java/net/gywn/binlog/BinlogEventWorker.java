package net.gywn.binlog;

import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import net.gywn.binlog.api.TargetHandler;
import net.gywn.binlog.beans.Binlog;
import net.gywn.binlog.beans.BinlogOperation;
import net.gywn.binlog.beans.BinlogTransaction;
import net.gywn.binlog.common.UldraConfig;
import net.gywn.binlog.common.UldraUtil;

public class BinlogEventWorker {
	private static final Logger logger = LoggerFactory.getLogger(BinlogEventWorker.class);
	private final String workerName;
	private final BlockingQueue<BinlogTransaction> queue;
	private final TargetHandler targetHandler;
	private final DataSource targetDataSource;
	private boolean processing = false;
	private Thread thread;

	@Getter
	private Binlog lastExecutedBinlog;

	public BinlogEventWorker(final int threadNumber, final UldraConfig uldraConfig) throws SQLException {
		this.workerName = String.format("w%03d", threadNumber);
		this.queue = new ArrayBlockingQueue<BinlogTransaction>(uldraConfig.getWokerQueueSize());
		this.targetHandler = uldraConfig.getTargetHandler();
		this.targetDataSource = uldraConfig.getTargetDataSource();
	}

	public void enqueue(final BinlogTransaction tx) {
		logger.debug("{}->enqueue()", workerName);
		while (true) {
			try {
				queue.add(tx);
				break;
			} catch (Exception e) {
//				logger.error("Enqueue error", e.getMessage());
				UldraUtil.sleep(100);
			}
		}
	}

	public void start() {
		thread = new Thread(new Runnable() {

			public void run() {
				while (true) {

					try {
						// ========================
						// dequeue
						// ========================
						BinlogTransaction binlogTransaction = queue.take();
						logger.debug("dequeue - {}", binlogTransaction);

						// ========================
						// transaction processing
						// ========================
						processing = true;
						while (true) {
							try {
								// begin
								transactionStart(binlogTransaction);

								// processing
//								binlogTransaction.prepare();
								for (final BinlogOperation binlogOperation : binlogTransaction.getBinlogOperations()) {
									binlogOperation.modify();

									// apply to target table
									binlogOperation.getBinlogOpType().executeBinlogOperation(binlogTransaction,
											binlogOperation, targetHandler);
								}

								// commit
								transactionCommit(binlogTransaction);
								lastExecutedBinlog = new Binlog(binlogTransaction.getPosition());
								break;
							} catch (Exception e) {
								logger.error(e.getMessage());
								transactionRollback(binlogTransaction);
								UldraUtil.sleep(1000);
							}
						}
						processing = false;
					} catch (InterruptedException e) {
						System.out.println("[dequeue]" + e);
					}
				}
			}

		}, workerName);
		thread.start();
		logger.info("{} started", workerName);

	}

	private void transactionStart(BinlogTransaction tx) throws Exception {
		logger.debug("transactionStart()");
		tx.setConnection(targetDataSource.getConnection());
		if (tx.isTransactional()) {
			targetHandler.begin(tx.getConnection());
		}
	}

	private void transactionCommit(BinlogTransaction tx) throws Exception {
		logger.debug("transactionCommit()");
		try {
			if (tx.isTransactional()) {
				targetHandler.commit(tx.getConnection());
			}
		} finally {
			tx.close();
		}
	}

	private void transactionRollback(BinlogTransaction tx) {
		logger.debug("transactionRollback()");
		try {
			if (tx.isTransactional()) {
				targetHandler.rollback(tx.getConnection());
			}
		} catch (Exception e) {
			logger.error("Rollback error", e.getMessage());
		} finally {
			tx.close();
		}
	}

	public int getJobCount() {
		return queue.size() + (processing ? 1 : 0);
	}

}
