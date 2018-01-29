package gi.kafka.model;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;

public class GInsDeserializer<T> implements Deserializer<T> {

	public void close() {
		// TODO Auto-generated method stub
		
	}

	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	public T deserialize(String topic, byte[] data) {
		try {
			final GInsData ginsData = new GInsData(data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// TODO Auto-generated method stub
		return null;
	}

}
