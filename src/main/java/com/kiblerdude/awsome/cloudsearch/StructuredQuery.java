package com.kiblerdude.awsome.cloudsearch;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;

/**
 * Builds structured queries for AWS Cloudsearch.
 * <p>
 * For cleaner and less verbose code, it is recommended to import the functions
 * statically:
 *
 * <pre>
 * import static io.awsome.cloudsearch.StructuredQuery.and;
 * import static io.awsome.cloudsearch.StructuredQuery.eq;
 * import static io.awsome.cloudsearch.StructuredQuery.range;
 * </pre>
 *
 * Example:
 *
 * <pre>
 * String structuredQuery = and(eq(&quot;field1&quot;, &quot;value&quot;), range(&quot;field2&quot;, 100, 200)).build();
 * </pre>
 *
 * This results in the following structured query:
 *
 * <pre>
 * (and (term field=field1 'value') (range field=field2 {100,200}))
 * </pre>
 *
 * @author kiblerj
 */
public final class StructuredQuery {

	// strings and dates need to be surrounded by single quotes
	private static final String QUOTED_FORMAT = "'%s'";

	// dates must be UTC (Coordinated Universal Time) and formatted according to
	// IETF RFC3339
	private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

	private static final ThreadLocal<SimpleDateFormat> formatter = ThreadLocal.withInitial(() -> {
    SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
    format.setTimeZone(TimeZone.getTimeZone("GMT"));
    return format;
  });

	private static final Joiner spaceJoiner = Joiner.on(" ");

	private final ExpressionOperator operator;
	private final ExpressionType type;
	private final Set<StructuredQuery> nested;
	private final String field;
	private final Optional<String> value;
	private final Optional<String> from;
	private final Optional<String> to;

	private Integer boost;

	/**
	 * Default constructor. Creates a <code>matchall</code> expression.
	 */
	private StructuredQuery() {
		this.operator = ExpressionOperator.NONE;
		this.type = ExpressionType.MATCHALL;
		this.nested = null;
		this.field = null;
		this.value = Optional.absent();
		this.from = Optional.absent();
		this.to = Optional.absent();
		this.boost = null;
	}

	/**
	 * Constructor for expressions.
	 *
	 * @param field The expression field
	 * @param value The expression value
	 */
	private StructuredQuery(ExpressionType type, String field, String value) {
		this.operator = ExpressionOperator.NONE;
		this.type = type;
		this.nested = null;
		this.field = Objects.requireNonNull(field);
		this.value = Optional.of(value);
		this.from = Optional.absent();
		this.to = Optional.absent();
		this.boost = null;
	}

	/**
	 * Constructor for <code>range</code> expressions.
	 *
	 * @param field The expression field
	 * @param from The from value of the expression
	 * @param to The to value of the expression
	 */
	private StructuredQuery(ExpressionType type, String field, String from, String to) {
		this.operator = ExpressionOperator.NONE;
		this.type = type;
		this.nested = null;
		this.field = Objects.requireNonNull(field);
		this.value = Optional.absent();
		this.from = Optional.fromNullable(from);
		this.to = Optional.fromNullable(to);
		this.boost = null;
	}

	/**
	 * Constructor for nested expressions.
	 *
	 * @param op The ExpressionOperator to apply
	 * @param expressions The nested expressions
	 */
	private StructuredQuery(ExpressionOperator op, StructuredQuery... expressions) {
		this.operator = op;
		this.type = ExpressionType.NONE;
		this.nested = ImmutableSet.copyOf(expressions);
		this.field = null;
		this.value = Optional.absent();
		this.from = Optional.absent();
		this.to = Optional.absent();
		this.boost = null;
	}

	/**
	 * Builds the AWS Cloudsearch Structured Query as a String.
	 *
	 * @return {@link String} representation of the AWS Cloudsearch Structured
	 *         Query. For example:
	 *
	 *         <pre>
	 * ( and ( term field='field1' 'value1' ) )
	 * </pre>
	 */
	public String build() {
		return toString();
	}

	/**
	 * Add a cloudsearch boost to this term.
	 * @param boost the boost weight
	 * @return builder pattern
	 */
	public StructuredQuery withBoost(int boost)
	{
		this.boost = boost;
		return this;
	}

	/**
	 * Compound expressions with the <code>and</code> operator. For example:
	 *
	 * <pre>
	 * ( and EXPRESSION1 EXPRESSION2 )
	 * </pre>
	 *
	 * @param expressions
	 *            One or more expressions to <code>and</code> together.
	 * @return {@link StructuredQuery}
	 */
	public static StructuredQuery and(StructuredQuery... expressions) {
		if (expressions.length == 0)
			throw new IllegalArgumentException("At least one expression is required");
		return new StructuredQuery(ExpressionOperator.AND, expressions);
	}

	/**
	 * Compound expressions with the <code>or</code> operator. For example:
	 *
	 * <pre>
	 * ( or EXPRESSION1 EXPRESSION2 )
	 * </pre>
	 *
	 * @param expressions
	 *            One or more expressions to <code>or</code> together.
	 * @return {@link StructuredQuery}
	 */
	public static StructuredQuery or(StructuredQuery... expressions) {
		if (expressions.length == 0)
			throw new IllegalArgumentException("At least one expression is required");
		return new StructuredQuery(ExpressionOperator.OR, expressions);
	}

	/**
	 * Negate an expression with the <code>not</code> operator. For example:
	 *
	 * <pre>
	 * ( not EXPRESSION )
	 * </pre>
	 *
	 * @param expression An expression to <code>not</code>.
	 * @return {@link StructuredQuery}
	 */
	public static StructuredQuery not(StructuredQuery expression) {
		return new StructuredQuery(ExpressionOperator.NOT, expression);
	}

	/**
	 * Creates a <code>matchall</code> search expression, which is used to
	 * return all the documents in a Cloudsearch domain. For example:
	 *
	 * <pre>
	 * (matchall)
	 * </pre>
	 *
	 * @return {@link StructuredQuery}
	 */
	public static StructuredQuery matchall() {
		return new StructuredQuery();
	}

	/**
	 * Creates a <code>phrase</code> search expression. For example:
	 *
	 * <pre>
	 * ( phrase field=field1 'the phrase' )
	 * </pre>
	 *
	 * @param field
	 *            The name of the indexed field to search for the phrase in.
	 * @param phrase
	 *            The phrase to search for.
	 * @return {@link StructuredQuery}
	 */
	public static StructuredQuery phrase(String field, String phrase) {
		return new StructuredQuery(ExpressionType.PHRASE, field,
				String.format(QUOTED_FORMAT, phrase));
	}

	/**
	 * Creates a <code>prefix</code> search expression. For example:
	 *
	 * <pre>
	 * ( prefix field=field1 'val' )
	 * </pre>
	 *
	 * @param field
	 *            The name of the indexed field to search for the prefix.
	 * @param prefix
	 *            The prefix to search for.
	 * @return {@link StructuredQuery}
	 */
	public static StructuredQuery prefix(String field, String prefix) {
		return new StructuredQuery(ExpressionType.PREFIX, field,
				String.format(QUOTED_FORMAT, prefix));
	}

	/**
	 * Creates a <code>term</code> search expression for String values. For
	 * example:
	 *
	 * <pre>
	 * ( term field=field1 'value' )
	 * </pre>
	 *
	 * @param field
	 *            The name of the indexed field to search for the term.
	 * @param value
	 *            The value to search for.
	 * @return {@link StructuredQuery}
	 */
	public static StructuredQuery eq(String field, String value) {
		return new StructuredQuery(ExpressionType.TERM, field,
				String.format(QUOTED_FORMAT, value));
	}

	/**
	 * Creates a <code>term</code> search expression for Long values. For
	 * example:
	 *
	 * <pre>
	 * ( term field=field1 100 )
	 * </pre>
	 *
	 * @param field
	 *            The name of the indexed field to search for the term.
	 * @param value
	 *            The value to search for.
	 * @return {@link StructuredQuery}
	 */
	public static StructuredQuery eq(String field, Long value) {
		return new StructuredQuery(ExpressionType.TERM, field,
				value.toString());
	}

	/**
	 * Creates a <code>term</code> search expression for Double values. For
	 * example:
	 *
	 * <pre>
	 * ( term field=field1 100.0 )
	 * </pre>
	 *
	 * @param field
	 *            The name of the indexed field to search for the term.
	 * @param value
	 *            The value to search for.
	 * @return {@link StructuredQuery}
	 */
	public static StructuredQuery eq(String field, Double value) {
		return new StructuredQuery(ExpressionType.TERM, field,
				value.toString());
	}

	/**
	 * Creates a <code>term</code> search expression for Date values. For
	 * example:
	 *
	 * <pre>
	 * ( term field=field1 '1970-01-01T00:00:00Z' )
	 * </pre>
	 *
	 * @param field
	 *            The name of the indexed field to search for the term.
	 * @param value
	 *            The value to search for.
	 * @return {@link StructuredQuery}
	 */
	public static StructuredQuery eq(String field, Date value) {
		String date = formatter.get().format(value);
		return new StructuredQuery(ExpressionType.TERM, field,
				String.format(QUOTED_FORMAT, date));
	}

	/**
	 * Creates a <code>range</code> search expression for String values. For
	 * example:
	 *
	 * <pre>
	 * ( range field=field1 { 'abc' , 'def' } )
	 * </pre>
	 *
	 * @param field
	 *            The name of the indexed field to search for the range.
	 * @param from
	 *            The value to search from.
	 * @param to
	 *            The value to search to.
	 * @return {@link StructuredQuery}
	 */
	public static StructuredQuery range(String field, String from, String to) {
		return new StructuredQuery(ExpressionType.RANGE, field,
				String.format(QUOTED_FORMAT, from), String.format(
						QUOTED_FORMAT, to));
	}

	/**
	 * Creates a <code>range</code> search expression for Long values. For
	 * example:
	 *
	 * <pre>
	 * ( range field=field1 { 100 , 200 } )
	 * </pre>
	 *
	 * @param field
	 *            The name of the indexed field to search for the range.
	 * @param from
	 *            The value to search from.
	 * @param to
	 *            The value to search to.
	 * @return {@link StructuredQuery}
	 */
	public static StructuredQuery range(String field, Long from, Long to) {
		return new StructuredQuery(ExpressionType.RANGE, field,
				from.toString(), to.toString());
	}

	/**
	 * Creates a <code>range</code> search expression for Double values. For
	 * example:
	 *
	 * <pre>
	 * ( range field=field1 { 0.0 , 100.0 } )
	 * </pre>
	 *
	 * @param field
	 *            The name of the indexed field to search for the range.
	 * @param from
	 *            The value to search from.
	 * @param to
	 *            The value to search to.
	 * @return {@link StructuredQuery}
	 */
	public static StructuredQuery range(String field, Double from, Double to) {
		return new StructuredQuery(ExpressionType.RANGE, field,
				from.toString(), to.toString());
	}

	/**
	 * Creates a <code>range</code> search expression for Date values. For
	 * example:
	 *
	 * <pre>
	 * ( range field=field1 { '1970-01-01T00:00:00Z' , '1971-01-01T00:00:00Z' } )
	 * </pre>
	 *
	 * @param field
	 *            The name of the indexed field to search for the range.
	 * @param from
	 *            The value to search from.
	 * @param to
	 *            The value to search to.
	 * @return {@link StructuredQuery}
	 */
	public static StructuredQuery range(String field, Date from, Date to) {
		String fromDate = formatter.get().format(from);
		String toDate = formatter.get().format(to);
		return new StructuredQuery(ExpressionType.RANGE, field,
				String.format(QUOTED_FORMAT, fromDate), String.format(
						QUOTED_FORMAT, toDate));
	}

	@Override
	public String toString() {

		// ensure we include the boost option if present
    final String boostStr = boost == null ? "" : "boost=" + boost;

		// there are a few conditions to check:
		// 1. matchall queries
		// 2. compound (nested) queries
		// 3. value queries
		// 4. range queries
		if (ExpressionType.MATCHALL.equals(type)) {
			return spaceJoiner.join("(", type.toString(),")");
		} else if (nested != null) {
			String nestedQuery = spaceJoiner.join(nested);
			List<String> queryParts = ImmutableList.of("(",
					operator.toString(), nestedQuery, ")");
			return spaceJoiner.join(queryParts);
		} else if (value.isPresent()) {
			List<String> queryParts = ImmutableList.of("(",
					type.toString(), "field=", field, boostStr, value.get(), ")");
			return spaceJoiner.join(queryParts);
		} else {
			List<String> queryParts = ImmutableList.of("(",
					type.toString(), "field=", field, boostStr, "{", from.or(""),
					",", to.or(""), "}", ")");
			return spaceJoiner.join(queryParts);
		}
	}
}
