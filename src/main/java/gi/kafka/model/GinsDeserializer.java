package gi.kafka.model;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;

public class GinsDeserializer<T> implements Deserializer<T> {

	public void close() {
		// TODO Auto-generated method stub
		
	}

	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	public T deserialize(String topic, byte[] data) {
		final GinsData ginsData = new GinsData(data);
		
		// TODO Auto-generated method stub
		return null;
	}

}
