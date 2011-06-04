package com.randomgraphs.avro;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.codehaus.jackson.JsonNode;

/**
 * Implementation of the Visitor pattern for Avro {@link IndexedRecord}
 * instances.
 *
 */
public class RecordVisitor {

	/**
	 * The Visitor interface that clients should implement.
	 */
	public static interface Visitor {
		void visitInt(Field field, Integer value);
		void visitLong(Field field, Long value);
		void visitFloat(Field field, Float value);
		void visitDouble(Field field, Double value);
		void visitBoolean(Field field, Boolean value);
		void visitBytes(Field field, ByteBuffer value);
		void visitString(Field field, CharSequence value);
		void visitRecord(Field field, IndexedRecord value);

		void visitArray(Field field, Collection<?> value);
		void visitMap(Field field, Map<Utf8, ?> value);
		void visitEnum(Field field, Object value);
	}
	
	@SuppressWarnings("unchecked")
	public static void visit(IndexedRecord record, Visitor visitor) {
		Schema schema = record.getSchema();
		if (schema.getType() == Schema.Type.RECORD) {
			for (Field field : schema.getFields()) {
				Schema fieldSchema = field.schema();
				// Special handling for unions
				if (fieldSchema.getType() == Type.UNION) {
					List<Schema> unionTypes = fieldSchema.getTypes();
					if (unionTypes.size() == 2) {
						if (unionTypes.get(0).getType() == Type.NULL) {
							fieldSchema = unionTypes.get(1);
						} else {
							fieldSchema = unionTypes.get(0);
						}
					}
				}
				Object value = record.get(field.pos());
				if (value == null) {
					if (field.defaultValue() != null) {
						JsonNode node = field.defaultValue();
						switch (fieldSchema.getType()) {
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
								continue;
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
				switch (fieldSchema.getType()) {
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
					visitor.visitString(field, (CharSequence) value);
					break;
				case ARRAY:
					visitor.visitArray(field, (Collection<?>) value);
					break;
				case MAP:
					visitor.visitMap(field, (Map<Utf8, ?>) value);
					break;
				case ENUM:
					visitor.visitEnum(field, value);
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
