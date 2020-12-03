package net.gywn.binlog.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.sql.DataSource;

import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import net.gywn.binlog.api.TargetHandler;
import net.gywn.binlog.beans.BinlogTable;

@Setter
@Getter
@ToString
public class UldraConfig {
	private static final Logger logger = LoggerFactory.getLogger(UldraConfig.class);

	private int workerCount = 4;
	private int wokerQueueSize = 500;
	private int exporterPort = 42000;
	private String binlogServer = "127.0.0.1:3306";
	private int binlogServerID = 5;
	private String binlogServerUsername = "repl";
	private String binlogServerPassword = "repl";
	private String binlogInfoFile;
	private String targetHandlerClassName;
	private String targetHandlerParamFile;
	private BinlogPolicy[] binlogPolicies;

	private DataSource targetDataSource;
	private TargetHandler targetHandler;
	private CaseInsensitiveMap<String, BinlogTable> tableMap = new CaseInsensitiveMap<String, BinlogTable>();
	private CaseInsensitiveMap<String, BinlogPolicy> binlogPolicyMap = new CaseInsensitiveMap<String, BinlogPolicy>();
	private CaseInsensitiveMap<String, ReplicatPolicy> replicatPolicyMap = new CaseInsensitiveMap<String, ReplicatPolicy>();

	public static UldraConfig loadUldraConfig(final String uldraConfigFile) throws Exception {
		try (FileInputStream fileInputStream = new FileInputStream(new File(uldraConfigFile));
				InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8")) {
			UldraConfig uldraConfig = new Yaml(new Constructor(UldraConfig.class)).loadAs(inputStreamReader,
					UldraConfig.class);
			return uldraConfig;
		}
	}

	public void modifyBinlogFile(final String binlogInfo) {
		String fileInfo = null;
		try {
			fileInfo = new String(Files.readAllBytes(Paths.get(binlogInfoFile)), StandardCharsets.UTF_8);
			logger.info("Current binlog file info {}", fileInfo);
		} catch (IOException e) {
			logger.error("Read on {} fail - ", binlogInfoFile, e.getMessage());
		}

		try {
			Files.write(Paths.get(binlogInfoFile), binlogInfo.replaceAll("\\s", "").getBytes(StandardCharsets.UTF_8));
		} catch (IOException e) {
			logger.error("Write on {} fail - ", binlogInfoFile, e.getMessage());
		}
	}

	public void init() throws Exception {
		logger.info("==================================");
		logger.info("Binlog policy initializing..");
		logger.info("==================================");
		for (BinlogPolicy binlogPolicy : binlogPolicies) {
			logger.info("[{}] Initializing..", binlogPolicy.getName());

			// ============================================
			// 트랜잭션 정책 중복 체크
			// ============================================
			if (binlogPolicyMap.containsKey(binlogPolicy.getName())) {
				throw new Exception("Duplicate binlog policy name" + binlogPolicy.getName() + ", exit");
			}
			binlogPolicyMap.put(binlogPolicy.getName(), binlogPolicy);

			// ============================================
			// 커스텀 row handler 로드
			// ============================================
			if (binlogPolicy.getRowHandlerClassName() == null
					|| binlogPolicy.getRowHandlerClassName().trim().length() == 0) {
				logger.info("- Row handler not defined, set default");
				binlogPolicy.setRowHandlerClassName("net.gywn.binlog.api.RowHandlerImpl");
			}

			logger.info("- Load rowHandler {}", binlogPolicy.getName());
			binlogPolicy.loadRowHandler();

			// ============================================
			// 타겟 테이블 정책 체크
			// ============================================
			if (binlogPolicy.getReplicatPolicies() == null) {
				logger.info("- NO replicate policy");
				binlogPolicy.setReplicatPolicies(new ReplicatPolicy[] { new ReplicatPolicy() });
			}

			for (ReplicatPolicy replicatPolicy : binlogPolicy.getReplicatPolicies()) {
				String targetName = replicatPolicy.getName();
				if (targetName == null) {
					logger.info("- Target name is null, get name from source table");
					targetName = binlogPolicy.getName().trim().replaceAll("(.*)\\.(.*)$", "$2");
				}
				targetName = targetName.toLowerCase();
				logger.info("- Set target table name {}", targetName);
				replicatPolicy.setName(targetName);
			}
		}

		logger.info("==================================");
		logger.info("Load target handler start");
		logger.info("==================================");
		try {
			this.targetHandler = (TargetHandler) (Class.forName(targetHandlerClassName)).newInstance();
			this.targetHandler.init(this);
			logger.info("{} loaded", targetHandlerClassName);
		} catch (Exception e) {
			logger.error("Load target handler fail - ", e.getMessage());
		}

		logger.info("==================================");
		logger.info("Define binlog info file");
		logger.info("==================================");
		if (this.binlogInfoFile == null) {
			logger.info("Binlog info file is not defined, set binlog-pos-{}:{}.info", binlogServer, binlogServerID);
			this.binlogInfoFile = String.format("binlog-pos-%s:%d.info", binlogServer, binlogServerID);
		}
	}
}