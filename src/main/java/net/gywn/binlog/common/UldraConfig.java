package net.gywn.binlog.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import net.gywn.binlog.api.RowHandler;
import net.gywn.binlog.api.TargetHandler;
import net.gywn.binlog.beans.Binlog;
import net.gywn.binlog.beans.BinlogTable;

@Setter
@Getter
@ToString
public class UldraConfig {
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
		} catch (IOException e) {
			fileInfo = "file read failed";
		}
		System.out.println(">> Current binlog file info: " + fileInfo);

		try {
			Files.write(Paths.get(binlogInfoFile), binlogInfo.replaceAll("\\s", "").getBytes(StandardCharsets.UTF_8));
		} catch (IOException e) {
			System.out.println("Initial binlog position failed");
		}
	}

	public void init() throws Exception {
		for (BinlogPolicy binlogPolicy : binlogPolicies) {
			System.out.println("Init - " + binlogPolicy);

			// ============================================
			// 트랜잭션 정책 중복 체크
			// ============================================
			String name = binlogPolicy.getName();
			if (binlogPolicyMap.containsKey(name)) {
				throw new Exception("Duplicate binlog policy name" + name + ", exit");
			}
			binlogPolicyMap.put(name, binlogPolicy);

			// ============================================
			// 커스텀 row handler 로드
			// ============================================
			if (binlogPolicy.getRowHandlerClassName() == null
					|| binlogPolicy.getRowHandlerClassName().trim().length() == 0) {
				System.out.println("[" + name + "] Row handler is not defined, set Default RowHandler");
				binlogPolicy.setRowHandlerClassName("net.gywn.binlog.api.RowHandlerImpl");
			}

			binlogPolicy.loadRowHandler();

			// ============================================
			// 타겟 테이블 정책 체크
			// ============================================
			for (ReplicatPolicy replicatPolicy : binlogPolicy.getReplicatPolicies()) {
				String targetName = replicatPolicy.getName();
				if (targetName == null) {
					targetName = binlogPolicy.getName().trim().replaceAll("(.*)\\.(.*)$", "$2");
				}
				targetName = targetName.toLowerCase();
				replicatPolicy.setName(targetName);
			}

			// ============================================
			// Target handler 로드 
			// ============================================
			this.targetHandler = (TargetHandler) (Class.forName(targetHandlerClassName)).newInstance();
			this.targetHandler.init(this);

			if (this.binlogInfoFile == null) {
				this.binlogInfoFile = String.format("binlog-pos-%s:%d.info", binlogServer, binlogServerID);
			}
		}
	}
}