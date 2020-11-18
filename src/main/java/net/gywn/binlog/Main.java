package net.gywn.binlog;

import java.util.concurrent.Callable;

import net.gywn.binlog.common.UldraConfig;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class Main implements Callable<Integer> {

	@Option(names = { "--config-file" }, description = "Config file", required = true)
	private String configFile;

	@Option(names = { "--worker-count" }, description = "Worker count", required = false)
	private Integer workerCount;

	@Option(names = { "--worker-queue-size" }, description = "Worker queue count", required = false)
	private Integer workerQueueSize;

	@Option(names = { "--binlog-info" }, description = "Binlog position info", required = false)
	private String binlogInfo;

	public static void main(String[] args) {
		Main main = new Main();
		if (args.length == 0) {
//			args = new String[] { "--config-file", "uldra-config.yml", "--binlog-info", "test.0001  :4" };
			args = new String[] { "--config-file", "uldra-config.yml" };
		}
		Integer exitCode = new CommandLine(main).execute(args);

		if (exitCode != 0) {
			System.exit(exitCode);
		}
	}

	@Override
	public Integer call() {
		try {
			UldraConfig uldraConfig = UldraConfig.loadUldraConfig(configFile);
			if (workerCount != null) {
				uldraConfig.setWorkerCount(workerCount);
			}
			if (workerQueueSize != null) {
				uldraConfig.setWorkerCount(workerQueueSize);
			}
			if (binlogInfo != null) {
				uldraConfig.modifyBinlogFile(binlogInfo);
			}

			uldraConfig.init();
			System.out.println("Config: " + uldraConfig);

			BinlogEventHandler binlogEventHandler = new BinlogEventHandler(uldraConfig);
			binlogEventHandler.start();

		} catch (Exception e) {
			System.out.println(e);
			return 1;
		}
		return 0;
	}
}