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
 * Helper class for defining an Avro record {@link Schema} dynamically.
 *
 */
public class RecordSchemaBuilder {

	private static final Schema STRING_SCHEMA = Schema.create(Type.STRING);
	private static final Schema INT_SCHEMA = Schema.create(Type.INT);
	private static final Schema LONG_SCHEMA = Schema.create(Type.LONG);
	private static final Schema FLOAT_SCHEMA = Schema.create(Type.FLOAT);
	private static final Schema DOUBLE_SCHEMA = Schema.create(Type.DOUBLE);
	private static final Schema NULL_SCHEMA = Schema.create(Type.NULL);

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
		return addField(name, STRING_SCHEMA, null);
	}
	
	public RecordSchemaBuilder optionalString(String name) {
		return optionalString(name, "");
	}
	
	public RecordSchemaBuilder optionalString(String name, String defaultValue) {
		return addOptionalField(name, STRING_SCHEMA, new TextNode(defaultValue));
	}

	public RecordSchemaBuilder requiredInt(String name) {
		return addField(name, INT_SCHEMA, null);
	}

	public RecordSchemaBuilder optionalInt(String name) {
		return optionalInt(name, 0);
	}

	public RecordSchemaBuilder optionalInt(String name, int defaultValue) {
		return addOptionalField(name, INT_SCHEMA, new IntNode(defaultValue));
	}

	public RecordSchemaBuilder requiredLong(String name) {
		return addField(name, LONG_SCHEMA, null);
	}

	public RecordSchemaBuilder optionalLong(String name) {
		return optionalLong(name, 0);
	}

	public RecordSchemaBuilder optionalLong(String name, long defaultValue) {
		return addOptionalField(name, LONG_SCHEMA, new LongNode(defaultValue));
	}

	public RecordSchemaBuilder requiredFloat(String name) {
		return addField(name, FLOAT_SCHEMA, null);
	}

	public RecordSchemaBuilder optionalFloat(String name) {
		return optionalFloat(name, 0.0f);
	}
	
	public RecordSchemaBuilder optionalFloat(String name, float defaultValue) {
		return addOptionalField(name, FLOAT_SCHEMA, new DoubleNode(defaultValue));
	}

	public RecordSchemaBuilder requiredDouble(String name) {
		return addField(name, DOUBLE_SCHEMA, null);
	}

	public RecordSchemaBuilder optionalDouble(String name) {
		return optionalDouble(name, 0.0);
	}
	
	public RecordSchemaBuilder optionalDouble(String name, double defaultValue) {
		return addOptionalField(name, DOUBLE_SCHEMA, new DoubleNode(defaultValue));
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
		} else { // Create an anonymous record
			return Schema.createRecord(fields);
		}
	}

	private RecordSchemaBuilder addField(String name, Schema schema, JsonNode defaultValue) {
		fields.add(new Field(name, schema, "", defaultValue));
		return this;
	}
	
	private RecordSchemaBuilder addOptionalField(String name, Schema schema, JsonNode defaultValue) {
		List<Schema> types = new ArrayList<Schema>();
		types.add(schema);
		types.add(NULL_SCHEMA);
		fields.add(new Field(name, Schema.createUnion(types), "", defaultValue));
		return this;
	}
}
