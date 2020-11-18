package net.gywn.binlog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import javax.sql.DataSource;

import io.prometheus.client.Summary;
import lombok.Getter;
import net.gywn.binlog.api.TargetHandler;
import net.gywn.binlog.beans.Binlog;
import net.gywn.binlog.beans.BinlogOperation;
import net.gywn.binlog.beans.BinlogTransaction;
import net.gywn.binlog.beans.TargetOperation;
import net.gywn.binlog.beans.TargetTable;
import net.gywn.binlog.common.UldraConfig;
import net.gywn.binlog.common.UldraUtil;

public class BinlogEventWorker {
	private final String threadName;
	private final BlockingQueue<BinlogTransaction> queue;
	private final TargetHandler targetHandler;
	private final DataSource targetDataSource;
	private boolean processing = false;
	private Thread thread;

	@Getter
	private Binlog lastExecutedBinlog;

	public BinlogEventWorker(final int threadNumber, final UldraConfig uldraConfig) throws SQLException {
		this.threadName = String.format("thread-%d", threadNumber);
		this.queue = new ArrayBlockingQueue<BinlogTransaction>(uldraConfig.getWokerQueueSize());
		this.targetHandler = uldraConfig.getTargetHandler();
		this.targetDataSource = uldraConfig.getTargetDataSource();
	}

	public void enqueue(final BinlogTransaction tx) {
		while (true) {
			try {
				queue.add(tx);
				break;
			} catch (Exception e) {
				System.out.println("[enqueue]" + e);
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
								// rollback
								System.out.println(e);
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

		}, threadName);
		thread.start();

	}

	private void transactionStart(BinlogTransaction tx) throws SQLException {
		tx.setConnection(targetDataSource.getConnection());
		if (tx.isTransactional()) {
			targetHandler.begin(tx.getConnection());
		}
	}

	private void transactionCommit(BinlogTransaction tx) throws SQLException {
		if (tx.isTransactional()) {
			targetHandler.commit(tx.getConnection());
			tx.close();
		}
	}

	private void transactionRollback(BinlogTransaction tx) {
		if (tx.isTransactional()) {
			try {
				targetHandler.rollback(tx.getConnection());
			} catch (SQLException e) {
				System.out.println("[rollback]" + e);
			}
			tx.close();
		}
	}

	public int getJobCount() {
		return queue.size() + (processing ? 1 : 0);
	}

}
