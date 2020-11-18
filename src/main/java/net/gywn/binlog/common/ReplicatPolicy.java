package net.gywn.binlog.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
public class ReplicatPolicy {
	private String name;
	private List<String> colums;
	private boolean softDelete = false;
	private boolean upsertMode = false;

	public void setName(String name) {
		this.name = name.toLowerCase();
	}
}
