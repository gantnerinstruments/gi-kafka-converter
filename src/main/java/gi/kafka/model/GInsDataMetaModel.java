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

public class GInsDataMetaModel {

	private MetaHeader[] header;
	//private final byte[] bytes;
	private final GInsData data;
	
	public GInsDataMetaModel(GInsData data) throws IOException {
		//System.out.println("meta: "+bytes.length);
		this.data = data;
		//this.bytes = data.getConverter().getMeta();

		final MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(data.getConverter().getMeta());
		this.header = MetaHeader.unpack(data, unpacker);
		unpacker.close();
	}
	
	public String getVersion() {
		return getPacketHeader().getVersion();
	}

	public int getDataCount() {
		return getPacketHeader().getDataCount();
	}

	public int getOffsetNS() {
		return getPacketHeader().getOffsetNS();
	}

	public double getSampleRate() {
		return getPacketHeader().getSampleRate();
	}

	public List<VariableHeader> getVariables() {
		return getPacketHeader().getVariables();
	}
	
	public VariableHeader getVariable(final int index) {
		return getPacketHeader().getVariable(index);
	}
	
	public MetaHeader getPacketHeader() {
		return this.header[0];
	}
	
}
