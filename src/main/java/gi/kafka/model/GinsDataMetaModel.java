package gi.kafka.model;

import java.io.IOException;
import java.util.List;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import gi.kafka.model.messages.MetaHeader;
import gi.kafka.model.messages.VariableHeader;

public class GinsDataMetaModel {

	private MetaHeader[] header;
	private final byte[] bytes;
	private final GinsData data;
	
	public GinsDataMetaModel(GinsData data) {
		//System.out.println("meta: "+bytes.length);
		this.data = data;
		this.bytes = data.getConverter().getMeta();
	}
	
	public MetaHeader getPacketHeader() throws JsonParseException, JsonMappingException, IOException {
		if (this.header == null) {
			final MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes);
			this.header = MetaHeader.unpack(data, unpacker);
			unpacker.close();
			
			//ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());
			//mapper.readValue(this.bytes, TKafkaPacketHeader[].class);
		}
		
		return this.header[0];
	}
	
}
