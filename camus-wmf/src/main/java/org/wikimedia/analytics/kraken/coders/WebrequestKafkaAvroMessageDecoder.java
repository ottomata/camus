package org.wikimedia.analytics.kraken.coders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Properties;

import kafka.message.Message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;
import com.linkedin.camus.schemaregistry.CachedSchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import com.linkedin.camus.etl.kafka.coders.LatestSchemaKafkaAvroMessageDecoder;

import org.apache.hadoop.io.Text;


public class WebrequestKafkaAvroMessageDecoder extends LatestSchemaKafkaAvroMessageDecoder {


    @Override
    public CamusWrapper<Record> decode(byte[] payload)
    {
        try
        {
            GenericDatumReader<Record> reader = new GenericDatumReader<Record>();

            Schema schema = super.registry.getLatestSchemaByTopic(super.topicName).getSchema();

            reader.setSchema(schema);

            return new CamusTimestampWrapper(reader.read(
                    null,
                    decoderFactory.jsonDecoder(
                            schema,
                            new String(
                                    payload,
                                    //Message.payloadOffset(message.magic()),
                                    Message.MagicOffset(),
                                    payload.length - Message.MagicOffset()
                            )
                    )
            ));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static class CamusTimestampWrapper extends CamusWrapper<Record> {
        public CamusTimestampWrapper(Record record) {
            super(record);
            Record header = (Record) super.getRecord().get("header");
            if (header != null) {
                if (header.get("server") != null) {
                    put(new Text("server"), new Text(header.get("server").toString()));
                }
                if (header.get("service") != null) {
                    put(new Text("service"), new Text(header.get("service").toString()));
                }
            }
        }

        @Override
        public long getTimestamp() {
            Record header = (Record) super.getRecord().get("header");
            String timestampString;

            if (header != null && header.get("time") != null) {
                timestampString = (String)header.get("time");
            } else if (super.getRecord().get("timestamp") != null) {
                timestampString = (String)super.getRecord().get("timestamp");
            } else {
                return System.currentTimeMillis();
            }
    //        [09/Aug/2013:16:03:58 +0000]

            try {
                return (new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss Z]").parse(timestampString).getTime());
            } catch (Exception e) {
                    throw new RuntimeException(e);
            }

        }
    }
}
