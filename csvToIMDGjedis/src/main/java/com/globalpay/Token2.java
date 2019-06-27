package com.globalpay;

import java.io.Serializable;

public class Token2 implements Serializable {

	private static final long serialVersionUID = 1L;
	private String gt;
	private String mt;

	public Token2() {
	}

	public Token2(String gt, String mt) {
		this.gt = gt;
		this.mt = mt;
	}

	public String getGt() {
		return gt;
	}

	public String getMt() {
		return mt;
	}

	@Override
	public String toString() {
		return "Token [gt=" + gt + ", mt=" + mt + "]";
	}

}