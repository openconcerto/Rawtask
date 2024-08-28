package com.rawtask.sql;

public class Record {
	private String pk, value;

	public Record(String pk, String value) {
		this.pk = pk;
		this.value = value;
	}

	public String getPk() {
		return this.pk;
	}

	public String getValue() {
		return this.value;
	}

	public void setPk(String pk) {
		this.pk = pk;
	}

	public void setValue(String value) {
		this.value = value;
	}

}
