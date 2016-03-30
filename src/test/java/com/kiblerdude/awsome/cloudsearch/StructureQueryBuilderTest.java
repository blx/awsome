package com.kiblerdude.awsome.cloudsearch;

import static com.kiblerdude.awsome.cloudsearch.StructuredQueryBuilder.and;
import static com.kiblerdude.awsome.cloudsearch.StructuredQueryBuilder.eq;
import static com.kiblerdude.awsome.cloudsearch.StructuredQueryBuilder.matchall;
import static com.kiblerdude.awsome.cloudsearch.StructuredQueryBuilder.not;
import static com.kiblerdude.awsome.cloudsearch.StructuredQueryBuilder.or;
import static com.kiblerdude.awsome.cloudsearch.StructuredQueryBuilder.phrase;
import static com.kiblerdude.awsome.cloudsearch.StructuredQueryBuilder.prefix;
import static com.kiblerdude.awsome.cloudsearch.StructuredQueryBuilder.range;
import static org.junit.Assert.assertEquals;
import java.util.Date;
import org.junit.Test;

public class StructureQueryBuilderTest {
	
	@Test
	public void testMatchAll() {
		String ma = matchall().toString();
		assertEquals("( matchall )", ma.toString());
	}
	
	@Test
	public void testEq() {
		Date date1 = new Date(0L);
		String eq1 = eq("field1", "value").toString();
		String eq2 = eq("field1", 10L).toString();
		String eq3 = eq("field1", 20.0).toString();
		String eq4 = eq("field1", date1).toString();
		
		assertEquals("( term field= field1  'value' )", eq1.toString());
		assertEquals("( term field= field1  10 )", eq2.toString());
		assertEquals("( term field= field1  20.0 )", eq3.toString());
		assertEquals("( term field= field1  '1970-01-01T00:00:00Z' )", eq4.toString());
	}
	
	@Test
	public void testRange() {
		Date date1 = new Date(0L);
		Date date2 = new Date(1000L);
		String range1 = range("field1", "abc", "def").toString();
		String range2 = range("field1", 10L, 20L).toString();
		String range3 = range("field1", 100.0, 200.0).toString();
		String range4 = range("field1", date1, date2).toString();
		
		assertEquals("( range field= field1  { 'abc' , 'def' } )", range1.toString());
		assertEquals("( range field= field1  { 10 , 20 } )", range2.toString());
		assertEquals("( range field= field1  { 100.0 , 200.0 } )", range3.toString());
		assertEquals("( range field= field1  { '1970-01-01T00:00:00Z' , '1970-01-01T00:00:01Z' } )", range4.toString());
	}
	
	@Test
	public void testPrefix() {
		String prefix1 = prefix("field1", "pre").toString();
		assertEquals("( prefix field= field1  'pre' )", prefix1.toString());
	}
	
	@Test
	public void testPhrase() {
		String phrase1 = phrase("field1", "hello world").toString();
		assertEquals("( phrase field= field1  'hello world' )", phrase1.toString());
	}
	
	@Test
	public void testCompoundAnd() {
		String compound1 = and(eq("field1", "value1"), eq("field2", "value2")).toString();
		assertEquals("( and ( term field= field1  'value1' ) ( term field= field2  'value2' ) )", compound1.toString());
	}

	@Test
	public void testCompoundOr() {
		String compound1 = or(eq("field1", "value1"), eq("field2", "value2")).toString();
		assertEquals("( or ( term field= field1  'value1' ) ( term field= field2  'value2' ) )", compound1.toString());
	}
	
	@Test
	public void testCompoundNot() {
		String compound1 = and(eq("field1", "value1"), not(eq("field2", "value2"))).toString();
		assertEquals("( and ( term field= field1  'value1' ) ( not ( term field= field2  'value2' ) ) )", compound1.toString());
	}
}
