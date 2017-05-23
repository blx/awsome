package com.kiblerdude.awsome.cloudsearch;

/**
 * Enumerates the type of searches supported in a Structured Query.
 * <p>
 * This enum is package protected.
 *
 * @author kiblerj
 *
 */
enum ExpressionType {
	MATCHALL("matchall"),
	NEAR("near"),
	PHRASE("phrase"),
	PREFIX("prefix"),
	TERM("term"),
	RANGE("range"),
	NONE("");
	private final String str;
	ExpressionType(String str) {
		this.str = str;
	}
	public String toString() {
		return str;
	}
}
