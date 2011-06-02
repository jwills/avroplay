package com.randomgraphs.avro;

import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;

import org.apache.hadoop.mapred.Reporter;

public class SumReducer<K> extends AvroReducer<K, IndexedRecord, Pair<K, IndexedRecord>> {

	private static class RecordAggregator implements RecordVisitor.Visitor {

		private final IndexedRecord aggregate;
		
		public RecordAggregator(Schema schema) {
			this.aggregate = new GenericData.Record(schema);
		}

		private RecordAggregator(IndexedRecord aggregate) {
			this.aggregate = aggregate;
		}

		public IndexedRecord getValue() {
			return aggregate;
		}

		private Number add(Number current, Number value) {
			if (value != null) {
				if (current == null) {
					current = 0;
				}
				if (value instanceof Integer) {
					return current.intValue() + value.intValue();
				} else if (value instanceof Long) {
					return current.longValue() + value.longValue();
				} else if (value instanceof Float) {
					return current.floatValue() + value.floatValue();
				} else if (value instanceof Double) {
					return current.doubleValue() + value.doubleValue();
				} else {
					throw new UnsupportedOperationException(
						"Unsupported Number type: " + value.getClass());
				}
			}
			return current;
		}

		private void visitNumber(Field field, Number value) {
			int pos = field.pos();
			Number current = (Number) aggregate.get(pos);
			aggregate.put(pos, add(current, value));
		}

		@Override
		public void visitInt(Field field, Integer value) {
			visitNumber(field, value);
		}

		@Override
		public void visitLong(Field field, Long value) {
			visitNumber(field, value);
		}

		@Override
		public void visitFloat(Field field, Float value) {
			visitNumber(field, value);
		}

		@Override
		public void visitDouble(Field field, Double value) {
			visitNumber(field, value);
		}

		@Override
		public void visitBoolean(Field field, Boolean value) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void visitBytes(Field field, ByteBuffer value) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void visitString(Field field, String value) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void visitRecord(Field field, IndexedRecord value) {
			IndexedRecord current = (IndexedRecord) aggregate.get(field.pos());
			if (current == null) {
				current = new GenericData.Record(field.schema());
			}
			RecordAggregator ra = new RecordAggregator(current);
			RecordVisitor.visit(value, ra);
			aggregate.put(field.pos(), current);
		}

		@Override
		public void visitArray(Field field, Object[] value) {
			int pos = field.pos();
			Object[] current = (Object[]) aggregate.get(pos);
			if (current == null) {
				current = new Object[value.length];
			}
			for (int i = 0; i < current.length; i++) {
				current[i] = add((Number) current[i], (Number) value[i]);
			}
			aggregate.put(pos, current);
		}

		@Override
		@SuppressWarnings("unchecked")
		public void visitMap(Field field, Map<String, Object> value) {
			int pos = field.pos();
			Map<String, Object> current = (Map<String, Object>) aggregate.get(pos);
			if (current == null) {
				current = new HashMap<String, Object>();
			}
			Set<String> keys = new HashSet<String>(current.keySet());
			keys.addAll(value.keySet());
			for (String key : keys) {
				Number currentValue = (Number) current.get(key);
				Number valueValue = (Number) value.get(key);
				current.put(key, add(currentValue, valueValue));
			}
			aggregate.put(pos, current);
		}

		@Override
		public void visitEnum(Field field, Enum<?> value) {
			throw new UnsupportedOperationException();
		}	
	}
	
	public void reduce(K key,
		Iterable<IndexedRecord> values,
		AvroCollector<Pair<K, IndexedRecord>> collector,
		Reporter reporter) {
		Iterator<IndexedRecord> iter = values.iterator();
		if (iter.hasNext()) {
			IndexedRecord first = iter.next();
			RecordAggregator aggregator = new RecordAggregator(first.getSchema());
			RecordVisitor.visit(first, aggregator);
			while (iter.hasNext()) {
				RecordVisitor.visit(iter.next(), aggregator);
			}
			IndexedRecord aggregateValue = aggregator.getValue();
			try {
				collector.collect(new Pair<K, IndexedRecord>(key, aggregateValue));
			} catch (IOException e) {
				reporter.incrCounter("sum-reducer", "collector-ioe", 1);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		// Dynamically define a schema in Java...
		Schema schema = new RecordSchemaBuilder()
			.requiredInt("foo", 2)
			.requiredFloat("bar")
			.optionalDouble("baz")
			.optionalLong("biz")
			.map("subkey", Schema.create(Type.INT))
			.build();
		
		// Create a couple of instantiations of the schema...
		List<IndexedRecord> records = new ArrayList<IndexedRecord>();
		GenericData.Record record = new GenericData.Record(schema);
		record.put("foo", 12);
		record.put("bar", 17.29f);
		Map<String, Object> secondaryKeys = new HashMap<String, Object>();
		secondaryKeys.put("k1", 12);
		secondaryKeys.put("k2", 27);
		record.put("subkey", secondaryKeys);
		records.add(record);

		record = new GenericData.Record(schema);
		record.put("bar", 1.0f);
		record.put("baz", 16.2);
		secondaryKeys = new HashMap<String, Object>();
		secondaryKeys.put("k1", 14);
		secondaryKeys.put("k3", 7);
		record.put("subkey", secondaryKeys);
		records.add(record);

		// ...and look at the output of reducing them.
		AvroCollector<Pair<String, IndexedRecord>> collector =
			new SystemOutAvroCollector<Pair<String, IndexedRecord>>();
		SumReducer<String> reducer = new SumReducer<String>();		
		reducer.reduce("test", records, collector, null);
	}
}
