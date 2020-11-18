package net.gywn.binlog.beans;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Data;

@Data
public class Binlog implements Comparable<Binlog> {
	private static final Logger logger = LoggerFactory.getLogger(Binlog.class);

	private String binlogFile;
	private long binlogPosition;

	public Binlog() {
	}

	public Binlog(final String binlogInfo) {
		String[] info = binlogInfo.trim().split(":");
		this.binlogFile = info[0];
		this.binlogPosition = Long.parseLong(info[1]);
	}

	public Binlog(final String binlogFile, final long binlogPosition) {
		this.binlogFile = binlogFile;
		this.binlogPosition = binlogPosition;
	}

	public int compareTo(Binlog o) {
		if (binlogFile.equals(o.binlogFile)) {
			return Long.compare(binlogPosition, o.binlogPosition);
		}
		return binlogFile.compareTo(o.binlogFile);
	}

	public String toString() {
		return String.format("%s:%d", this.binlogFile, this.binlogPosition);
	}

	public static Binlog[] read(String binlogInfoFile) throws IOException {
		String binlogInfo = new String(Files.readAllBytes(Paths.get(binlogInfoFile)), StandardCharsets.UTF_8);
		String[] infos = binlogInfo.split(",");
		Binlog[] binlogs = { new Binlog(infos[0]), new Binlog(infos[infos.length - 1]) };
		return binlogs;
	}

	public static void flush(Binlog binlog, String binlogInfoFile) throws IOException {
		flush(binlog, null, binlogInfoFile);
	}

	public static void flush(Binlog binlog, Binlog targetBinlog, String binlogInfoFile) throws IOException {
		String binlogInfo = binlog.toString();
		if (targetBinlog != null) {
			binlogInfo += "," + targetBinlog.toString();
		}
		Files.write(Paths.get(binlogInfoFile), binlogInfo.getBytes(StandardCharsets.UTF_8));
	}
}
