package org.wikimedia.analytics.kraken.schemaregistry;

import org.apache.avro.Schema;

import org.wikimedia.analytics.kraken.records.Webrequest;
import com.linkedin.camus.schemaregistry.MemorySchemaRegistry;

/**
 * This is a little dummy registry that just uses a memory-backed schema
 * registry to store two dummy Avro schemas. You can use this with
 * camus.properties
 */
public class KrakenSchemaRegistry extends MemorySchemaRegistry<Schema> {
	public KrakenSchemaRegistry() {
		super();
		super.register("varnish2", Webrequest.newBuilder().build().getSchema());
	}
}
