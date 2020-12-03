package net.gywn.binlog;

import java.util.concurrent.Callable;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.gywn.binlog.common.UldraConfig;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class Main implements Callable<Integer> {
	private static final Logger logger = LoggerFactory.getLogger(Main.class);

	@Option(names = { "--config-file" }, description = "Config file", required = true)
	private String configFile;

	@Option(names = { "--worker-count" }, description = "Worker count", required = false)
	private Integer workerCount;

	@Option(names = { "--worker-queue-size" }, description = "Worker queue count", required = false)
	private Integer workerQueueSize;

	@Option(names = { "--binlog-info" }, description = "Binlog position info", required = false)
	private String binlogInfo;

	static {
		try {
			String loggingConfigFile = System.getProperty("java.util.logging.config.file");
			if(loggingConfigFile == null) {
				loggingConfigFile = "log4j.properties";
			}
			PropertyConfigurator.configure(loggingConfigFile);
		} catch (Exception e) {
		}
	}

	public static void main(String[] args) {

		Main main = new Main();
		if (args.length == 0) {
			args = new String[] { "--config-file", "uldra-config.yml" };
		}
		Integer exitCode = new CommandLine(main).execute(args);

		if (exitCode != 0) {
			logger.error("exit code: {}", exitCode);
		}
	}

	@Override
	public Integer call() {

		try {
			logger.info("Load from {}", configFile);
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

			logger.info(uldraConfig.toString());
			BinlogEventHandler binlogEventHandler = new BinlogEventHandler(uldraConfig);
			binlogEventHandler.start();

		} catch (Exception e) {
			logger.error(e.getMessage());
			return 1;
		}
		return 0;
	}
}