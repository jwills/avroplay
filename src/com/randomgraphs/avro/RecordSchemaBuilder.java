package com.randomgraphs.avro;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.LongNode;
import org.codehaus.jackson.node.TextNode;

/**
 * Utility class for defining an Avro {@link Schema} for a record dynamically.
 *
 */
public class RecordSchemaBuilder {

	private static final Schema STRING_SCHEMA = Schema.create(Type.STRING);
	private static final Schema INT_SCHEMA = Schema.create(Type.INT);
	private static final Schema LONG_SCHEMA = Schema.create(Type.LONG);
	private static final Schema FLOAT_SCHEMA = Schema.create(Type.FLOAT);
	private static final Schema DOUBLE_SCHEMA = Schema.create(Type.DOUBLE);

	private final String recordName;
	private String namespace;
	private String doc;
	private boolean isError;
	private final List<Field> fields = new ArrayList<Field>();

	public RecordSchemaBuilder() {
		this(null);
	}
	
	public RecordSchemaBuilder(String recordName) {
		this.recordName = recordName;
	}

	private void checkRecordName(String field) {
		if (recordName == null) {
			throw new IllegalStateException(
				String.format("Cannot specify field %s on an anonymous record", field));
		}
	}
	
	public RecordSchemaBuilder doc(String doc) {
		checkRecordName("doc");
		this.doc = doc;
		return this;
	}

	public RecordSchemaBuilder namespace(String namespace) {
		checkRecordName("namespace");
		this.namespace = namespace;
		return this;
	}
	
	public RecordSchemaBuilder isError() {
		checkRecordName("isError");
		this.isError = true;
		return this;
	}

	public RecordSchemaBuilder requiredString(String name) {
		return requiredString(name, "");
	}

	public RecordSchemaBuilder requiredString(String name, String defaultValue) {
		return addField(name, STRING_SCHEMA, new TextNode(defaultValue));
	}
	
	public RecordSchemaBuilder optionalString(String name) {
		return addField(name, STRING_SCHEMA, null);
	}

	public RecordSchemaBuilder requiredInt(String name) {
		return requiredInt(name, 0);
	}

	public RecordSchemaBuilder requiredInt(String name, int defaultValue) {
		return addField(name, INT_SCHEMA, new IntNode(defaultValue));
	}

	public RecordSchemaBuilder optionalInt(String name) {
		return addField(name, INT_SCHEMA, null);
	}
	
	public RecordSchemaBuilder requiredLong(String name) {
		return requiredLong(name, 0L);
	}

	public RecordSchemaBuilder requiredLong(String name, long defaultValue) {
		return addField(name, LONG_SCHEMA, new LongNode(defaultValue));
	}
	
	public RecordSchemaBuilder optionalLong(String name) {
		return addField(name, LONG_SCHEMA, null);
	}
	
	public RecordSchemaBuilder requiredFloat(String name) {
		return requiredFloat(name, 0.0f);
	}
	
	public RecordSchemaBuilder requiredFloat(String name, float defaultValue) {
		return addField(name, FLOAT_SCHEMA, new DoubleNode(defaultValue));
	}

	public RecordSchemaBuilder optionalFloat(String name) {
		return addField(name, FLOAT_SCHEMA, null);
	}

	public RecordSchemaBuilder requiredDouble(String name) {
		return requiredDouble(name, 0.0);
	}
	
	public RecordSchemaBuilder requiredDouble(String name, double defaultValue) {
		return addField(name, DOUBLE_SCHEMA, new DoubleNode(defaultValue));
	}

	public RecordSchemaBuilder optionalDouble(String name) {
		return addField(name, DOUBLE_SCHEMA, null);
	}

	public RecordSchemaBuilder map(String name, Schema valueSchema) {
		return addField(name, Schema.createMap(valueSchema), null);
	}
	
	public RecordSchemaBuilder array(String name, Schema valueSchema) {
		return addField(name, Schema.createArray(valueSchema), null);
	}

	public Schema build() {
		if (recordName != null) {
			Schema schema = Schema.createRecord(recordName, doc, namespace, isError);
			schema.setFields(fields);
			return schema;
		} else {
			return Schema.createRecord(fields);
		}
	}

	private RecordSchemaBuilder addField(String name, Schema schema, JsonNode defaultValue) {
		fields.add(new Field(name, schema, "", defaultValue));
		return this;
	}
}
