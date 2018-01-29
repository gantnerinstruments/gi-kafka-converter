package gi.kafka.model;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import gi.kafka.jni.GInsDataKafkaConverter;
import gi.kafka.model.messages.VariableHeader;

public class GInsData {

	private byte[] data;
	private GInsDataKafkaConverter converter;
	private GInsDataMetaModel metaModel;
	
	public GInsData(String directory) throws IOException {
		this.data = getBytes(directory);
		this.load();
	}
	
	public GInsData(final byte[] data) throws IOException {
		this.data = data;
		this.load();
	}
	
	private void load() throws IOException {
		this.converter = new GInsDataKafkaConverter(data);
		this.metaModel = new GInsDataMetaModel(this);
		this.converter.free();
	}
	
	public GInsDataKafkaConverter getConverter() {
		return this.converter;
	}
	
	public GInsDataMetaModel getMeta() {
		return this.metaModel;
	}
	
	public List<VariableHeader> getVariables() {
		return this.metaModel.getVariables();
	}
	
	public VariableHeader getVariable(int index) {
		return this.metaModel.getVariable(index);
	}
	
	private MappedByteBuffer getByteBuffer(String dir) throws IOException {
		final RandomAccessFile in = new RandomAccessFile(new File(dir), "r");

		// fastest way of reading large files
		// https://stackoverflow.com/questions/9046820/fastest-way-to-incrementally-read-a-large-file
		final FileChannel inChannel = in.getChannel();
		final MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
		inChannel.close();
		in.close();
		
		return buffer;
	}

	private byte[] getBytes(String dir) throws IOException {
		final MappedByteBuffer buffer = getByteBuffer(dir);
		final byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);

		return bytes;
	}
	
	private byte[] getBytes2(String dir) throws IOException {
		final RandomAccessFile in = new RandomAccessFile(new File(dir), "r");
		
		// risky, because I don't do any buffering
		// but enough for testing purposes!
		byte[] data = null;
		try {
			data = new byte[(int)in.length()];
			in.read(data);
		} finally {
			try {
				in.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return data;
	}
}
