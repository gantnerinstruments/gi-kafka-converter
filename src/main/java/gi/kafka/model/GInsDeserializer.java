package gi.kafka.model;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;

import gi.kafka.model.messages.VariableHeader;

public class GInsDeserializer<T> implements Deserializer<T> {

	public void close() {
		
	}

	public void configure(Map<String, ?> configs, boolean isKey) {
		
	}

	public T deserialize(String topic, byte[] data) {
		GInsData ginsData = null;
		try {
			// initialize GInsData with byte array or file name
			ginsData = new GInsData(data);
			
			// obtain meta information and variable list
			final GInsDataMetaModel meta = ginsData.getMeta();
			final List<VariableHeader> variables = meta.getVariables();
			
			// select variable by index
			final VariableHeader myVar = variables.get(0);
			
			// get data in Number array (generic way)
			myVar.getGenericData();
			
			// get float data
			if (myVar.getDataType() == VariableHeader.DATA_TYPE_Float) {
				try {
					float[] floatVals = myVar.getFloatData();
				} catch (InvalidDataStreamException e) {
					e.printStackTrace();
				}
			}
			
		// couldn't read byte data
		} catch (IOException e) {
			e.printStackTrace();
			
		// free memory allocations in c++
		} finally {
			if (ginsData != null) {
				ginsData.free();
			}
		}
		
		// TODO Auto-generated method stub
		return null;
	}

}
