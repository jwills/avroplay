package com.randomgraphs.avro;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;


import org.apache.hadoop.mapred.Reporter;

/**
 * An implementation of {@link AvroReducer} that aggregates all of the numeric
 * fields in the {@link IndexedRecord} instances associated with the key by
 * adding their values together, including support for aggregating values
 * stored in sub-records, arrays, and maps defined within the record.
 *
 * @param <K> The data type of the key.
 */
public class SumReducer<K> extends AvroReducer<K, IndexedRecord, Pair<K, IndexedRecord>> {

	/**
	 * The implementation of the {@link RecordVisitor.Visitor} pattern that
	 * handles the actual aggregation of numeric values from the input
	 * records.
	 *
	 */
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
		public void visitString(Field field, CharSequence value) {
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
			aggregate.put(field.pos(), ra.getValue());
		}

		@Override
		@SuppressWarnings("unchecked")
		public void visitArray(Field field, Collection<?> value) {
			int pos = field.pos();
			List<Number> current = (List<Number>) aggregate.get(pos);
			if (current == null) {
				current = new ArrayList<Number>(value.size());
			}
			int i = 0;
			for (Object v : value) {
				current.set(i, add(current.get(i), (Number) v));
			}
			aggregate.put(pos, current);
		}

		@Override
		@SuppressWarnings("unchecked")
		public void visitMap(Field field, Map<Utf8, ?> value) {
			int pos = field.pos();
			Map<Utf8, Object> current = (Map<Utf8, Object>) aggregate.get(pos);
			if (current == null) {
				current = new HashMap<Utf8, Object>();
			}
			Set<Utf8> keys = new HashSet<Utf8>(current.keySet());
			keys.addAll(value.keySet());
			for (Utf8 key : keys) {
				Number currentValue = (Number) current.get(key);
				Number valueValue = (Number) value.get(key);
				current.put(key, add(currentValue, valueValue));
			}
			aggregate.put(pos, current);
		}

		@Override
		public void visitEnum(Field field, Object value) {
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
		Schema schema = new RecordSchemaBuilder("MySchema")
			.requiredInt("foo")
			.requiredFloat("bar")
			.optionalLong("biz")
			.map("subkey", Schema.create(Type.INT))
			.build();
		
		File recordFile = new File("testrecs");
		DatumWriter<IndexedRecord> writer = new GenericDatumWriter<IndexedRecord>();
		DataFileWriter<IndexedRecord> fileWriter = new DataFileWriter<IndexedRecord>(writer);
		fileWriter.create(schema, recordFile);

		// Create a couple of instantiations of the schema and write them out
		GenericData.Record record = new GenericData.Record(schema);
		record.put("foo", 12);
		record.put("bar", 17.29f);
		Map<String, Object> secondaryKeys = new HashMap<String, Object>();
		secondaryKeys.put("k1", 12);
		secondaryKeys.put("k2", 27);
		record.put("subkey", secondaryKeys);
		fileWriter.append(record);

		record = new GenericData.Record(schema);
		record.put("foo", 2);
		record.put("bar", 1.0f);
		record.put("biz", 10L);
		secondaryKeys = new HashMap<String, Object>();
		secondaryKeys.put("k1", 14);
		secondaryKeys.put("k3", 7);
		record.put("subkey", secondaryKeys);
		fileWriter.append(record);
		fileWriter.close();
		
		// Read everything back in
		List<IndexedRecord> records = new ArrayList<IndexedRecord>();
		DatumReader<IndexedRecord> reader = new GenericDatumReader<IndexedRecord>(schema);
		InputStream in = new BufferedInputStream(new FileInputStream(recordFile));
		DataFileStream<IndexedRecord> stream = new DataFileStream<IndexedRecord>(in, reader);
		for (IndexedRecord r : stream)
			records.add(r);

		// ...and look at the output of reducing them.
		AvroCollector<Pair<Long, IndexedRecord>> collector =
			new SystemOutAvroCollector<Pair<Long, IndexedRecord>>();
		SumReducer<Long> reducer = new SumReducer<Long>();		
		reducer.reduce(1729L, records, collector, null);
		// Output is:
		// {"key": 1729, "value": {"foo": 14, "bar": 18.29, "biz": 10, "subkey": {"k3": 7, "k1": 26, "k2": 27}}}

		// cleanup
		recordFile.delete();
	}
}
