package com.kiblerdude.awsome.cloudsearch;

/**
 * Enumerates the logical operators supported in a Structured Query.
 * <p>
 * This enum is package protected.
 *
 * @author kiblerj
 *
 */
enum ExpressionOperator {
	AND("and"),
	OR("or"),
	NOT("not"),
	NONE("");
	private final String str;
	ExpressionOperator(String str) {
		this.str = str;
	}
	public String toString() {
		return str;
	}
}
