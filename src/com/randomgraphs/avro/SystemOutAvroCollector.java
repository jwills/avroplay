package com.randomgraphs.avro;

import java.io.IOException;

import org.apache.avro.mapred.AvroCollector;

/**
 * Testing class for examining the output of an Avro MapReduce.
 *
 * @param <T> The type of the final output from the MapReduce.
 */
public class SystemOutAvroCollector<T> extends AvroCollector<T> {
	@Override
	public void collect(T datum) throws IOException {
		System.out.println(datum);
	}
}
