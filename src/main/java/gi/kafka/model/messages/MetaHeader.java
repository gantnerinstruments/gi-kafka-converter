package gi.kafka.model.messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.msgpack.core.MessageUnpacker;

import com.fasterxml.jackson.annotation.JsonProperty;

import gi.kafka.model.GinsData;

public class MetaHeader {

	public static MetaHeader[] unpack(GinsData data, MessageUnpacker unpacker) throws IOException {
		final int elements = unpacker.unpackArrayHeader();
		final MetaHeader[] ph = new MetaHeader[elements];
		
		// TODO: why is this 1 and not headers?
		//System.out.println("meta: "+elements);
		for (int i = 0; i < 1; i++) {
			ph[i] = new MetaHeader(data);
			ph[i].setVersion(unpacker.unpackString());
			ph[i].setDataCount(unpacker.unpackInt());
			ph[i].setOffsetNS(unpacker.unpackInt());
			ph[i].setSampleRate(unpacker.unpackDouble());
			ph[i].variables = new ArrayList<VariableHeader>();
			
			final VariableHeader vars[] = VariableHeader.unpack(data, unpacker);
			for (VariableHeader var : vars)
				ph[i].variables.add(var);
		}
		
		return ph;
	}
	
	private final GinsData data;
	public MetaHeader(GinsData data) {
		super();
		this.data = data;
	}

	@JsonProperty("Version")
	private String version;

	@JsonProperty("DataCount")
	private int dataCount;

	@JsonProperty("Offset_ns")
	private int offsetNS;

	@JsonProperty("SampleRate")
	private double sampleRate;

	@JsonProperty("Variables")
	private List<VariableHeader> variables;

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public int getDataCount() {
		return dataCount;
	}

	public void setDataCount(int dataCount) {
		this.dataCount = dataCount;
	}

	public int getOffsetNS() {
		return offsetNS;
	}

	public void setOffsetNS(int offsetNS) {
		this.offsetNS = offsetNS;
	}

	public double getSampleRate() {
		return sampleRate;
	}

	public void setSampleRate(double sampleRate) {
		this.sampleRate = sampleRate;
	}

	public List<VariableHeader> getVariables() {
		return variables;
	}
	
	public VariableHeader getVariable(int index) {
		return variables.get(index);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + dataCount;
		result = prime * result + offsetNS;
		long temp;
		temp = Double.doubleToLongBits(sampleRate);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((variables == null) ? 0 : variables.hashCode());
		result = prime * result + ((version == null) ? 0 : version.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MetaHeader other = (MetaHeader) obj;
		if (dataCount != other.dataCount)
			return false;
		if (offsetNS != other.offsetNS)
			return false;
		if (Double.doubleToLongBits(sampleRate) != Double.doubleToLongBits(other.sampleRate))
			return false;
		if (variables == null) {
			if (other.variables != null)
				return false;
		} else if (!variables.equals(other.variables))
			return false;
		if (version == null) {
			if (other.version != null)
				return false;
		} else if (!version.equals(other.version))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "TKafkaPacketHeader [version=" + version + ", dataCount=" + dataCount + ", offsetNS=" + offsetNS
				+ ", sampleRate=" + sampleRate + ", variables=" + variables + "]";
	}

}
