package org.wikimedia.analytics.kraken.coders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.text.SimpleDateFormat;

import com.google.gson.JsonParser;
import com.google.gson.JsonObject;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;

import org.apache.log4j.Logger;


public class JsonMessageDecoder extends MessageDecoder<byte[], String> {
	private static org.apache.log4j.Logger log = Logger.getLogger(JsonMessageDecoder.class);

	@Override
	public CamusWrapper<String> decode(byte[] payload) {
		long timestamp = 0;
		String payloadString;
		JsonObject jsonObject;

		payloadString =  new String(payload);
		log.debug("Read new string:\n" + payloadString);

		// parse the payload into a JsonObject.
		try {
			jsonObject = new JsonParser().parse(payloadString).getAsJsonObject();
		} catch (RuntimeException e) {
			log.error("Caught exception while parsing JSON string '" + payloadString + "'.");
			throw new RuntimeException(e);
		}

		// Attempt to read and parse the timestamp element into a long.
	    if (jsonObject.has("timestamp")) {
	     	String timestampString = jsonObject.get("timestamp").getAsString();
	     	try {
				timestamp = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss Z]").parse(timestampString).getTime();
			} catch (Exception e) {
	        	log.error("Could not parse timestamp '" + timestampString + "' while decoding JSON message.");
			}
	    }

        // If timestamp wasn't set in the above block,
        // then set it to current time.
		if (timestamp == 0) {
			log.warn("Couldn't find timestamp in JSON message, defaulting to current time.");
    		timestamp = System.currentTimeMillis();
    	}

        return new CamusWrapper<String>(payloadString, timestamp);
	}
}
