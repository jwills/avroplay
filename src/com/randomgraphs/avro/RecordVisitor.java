package com.randomgraphs.avro;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.codehaus.jackson.JsonNode;

public class RecordVisitor {

	public static interface Visitor {
		void visitInt(Field field, Integer value);
		void visitLong(Field field, Long value);
		void visitFloat(Field field, Float value);
		void visitDouble(Field field, Double value);
		void visitBoolean(Field field, Boolean value);
		void visitBytes(Field field, ByteBuffer value);
		void visitString(Field field, String value);
		void visitRecord(Field field, IndexedRecord value);

		void visitArray(Field field, Object[] value);
		void visitMap(Field field, Map<String, Object> value);
		void visitEnum(Field field, Enum<?> value);
	}
	
	@SuppressWarnings("unchecked")
	public static void visit(IndexedRecord record, Visitor visitor) {
		Schema schema = record.getSchema();
		if (schema.getType() == Schema.Type.RECORD) {
			for (Field field : schema.getFields()) {
				Object value = record.get(field.pos());
				if (value == null) {
					if (field.defaultValue() != null) {
						JsonNode node = field.defaultValue();
						switch (field.schema().getType()) {
						case INT:
						case LONG:
						case FLOAT:
						case DOUBLE:
						case BOOLEAN:
							value = node.getNumberValue();
							break;
						case BYTES:
							try {
								value = ByteBuffer.wrap(node.getBinaryValue());
							} catch (IOException e) {
								value = null;
							}
							break;
						case STRING:
							value = node.getTextValue();
							break;
						default:
							continue;
						}
					} else {
						continue;
					}
				}
				switch (field.schema().getType()) {
				case INT:
					visitor.visitInt(field, (Integer) value);
					break;
				case LONG:
					visitor.visitLong(field, (Long) value);
					break;
				case FLOAT:
					visitor.visitFloat(field, (Float) value);
					break;
				case DOUBLE:
					visitor.visitDouble(field, (Double) value);
					break;
				case BOOLEAN:
					visitor.visitBoolean(field, (Boolean) value);
					break;
				case BYTES:
					visitor.visitBytes(field, (ByteBuffer) value);
					break;
				case STRING:
					visitor.visitString(field, (String) value);
					break;
				case ARRAY:
					visitor.visitArray(field, (Object[]) value);
					break;
				case MAP:
					visitor.visitMap(field, (Map<String, Object>) value);
					break;
				case ENUM:
					visitor.visitEnum(field, (Enum<?>) value);
					break;
				case RECORD:
					visitor.visitRecord(field, (IndexedRecord) value);
					break;
					default:
						//TODO: Write the cases for union and fixed
						break;
				}
			}
		}
	}
}
