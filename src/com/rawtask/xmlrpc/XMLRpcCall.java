package com.rawtask.xmlrpc;

import java.util.List;

public class XMLRpcCall {
	private final String method;
	private final List<?> params;

	public XMLRpcCall(String method, List<?> params) {
		this.method = method;
		this.params = params;
	}

	public String getMethod() {
		return this.method;
	}

	public List<?> getParams() {
		return this.params;
	}
}
