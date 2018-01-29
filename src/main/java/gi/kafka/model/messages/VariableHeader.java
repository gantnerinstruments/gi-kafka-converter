package gi.kafka.model.messages;

import java.io.IOException;
import java.util.Iterator;

import org.msgpack.core.MessageUnpacker;

import com.fasterxml.jackson.annotation.JsonProperty;

import gi.kafka.jni.GInsDataKafkaConverter;
import gi.kafka.model.GInsData;
import gi.kafka.model.InvalidDataStreamException;

public class VariableHeader {

	private static final int TYPE_No = 0;
	private static final int TYPE_Boolean = 1;
	private static final int TYPE_SignedInt8 = 2;
	private static final int TYPE_UnSignedInt8 = 3;
	private static final int TYPE_SignedInt16 = 4;
	private static final int TYPE_UnSignedInt16 = 5;
	private static final int TYPE_SignedInt32 = 6;
	private static final int TYPE_UnSignedInt32 = 7;
	private static final int TYPE_Float = 8;
	private static final int TYPE_BitSet8 = 9;
	private static final int TYPE_BitSet16 = 10;
	private static final int TYPE_BitSet32 = 11;
	private static final int TYPE_Double = 12;
	private static final int TYPE_SignedInt64 = 13;
	private static final int TYPE_UnSignedInt64 = 14;
	private static final int TYPE_BitSet64 = 15;
	
	private static final String[] types = {
		"none",
		"boolean",
		"signed int 8",
		"unsigned int 8",
		"signed int 16",
		"unsigned int 16",
		"signed int 32",
		"unsinged int 32",
		"float",
		"bitset 8",
		"bitset 16",
		"bitset 32",
		"double",
		"signed int 64",
		"unsigned int 64",
		"bitset 64"
	};
	
	
	public static VariableHeader[] unpack(GInsData data, MessageUnpacker unpacker) throws IOException {
		final int headers = unpacker.unpackArrayHeader();
		final VariableHeader[] vh = new VariableHeader[headers];
		
		//System.out.println("headers: "+headers);
		for (int i = 0; i < headers; i++) {
			vh[i] = new VariableHeader(data, i);
			
			final int elements = unpacker.unpackArrayHeader();
			//System.out.println("unpacking: "+i+": "+elements);
			
			vh[i].setDataDirection(unpacker.unpackInt());
			vh[i].setDataType(unpacker.unpackInt());
			vh[i].setFieldLength(unpacker.unpackInt());
			vh[i].setId(unpacker.unpackString());
			vh[i].setName(unpacker.unpackString());
			vh[i].setPrecision(unpacker.unpackInt());
			vh[i].setUnit(unpacker.unpackString());
			vh[i].setVariableType(unpacker.unpackInt());
		}
		
		return vh;
	}
	
	private final int variableIndex;
	private final GInsData data;
	public VariableHeader(GInsData data, int variableIndex) {
		super();
		this.data = data;
		this.variableIndex = variableIndex;
	}


	
	public boolean[] getBooleanData() throws InvalidDataStreamException {
		if (this.getDataType() != TYPE_Boolean)
			throw new InvalidDataStreamException("Trying to read boolean data from "+types[this.getDataType()]+".");
		return this.data.getConverter().getVariableDataBoolean(this.variableIndex);
	}
	
	public byte[] getByteData() {
		return this.data.getConverter().getVariableDataByte(this.variableIndex);
	}

	public short[] getShortData() throws InvalidDataStreamException {
		if (this.getDataType() != TYPE_UnSignedInt16 && this.getDataType() != TYPE_SignedInt16 && this.getDataType() != TYPE_BitSet16)
			throw new InvalidDataStreamException("Trying to read short data from "+types[this.getDataType()]+".");
		return this.data.getConverter().getVariableDataShort(this.variableIndex);
	}
	
	public float[] getFloatData() throws InvalidDataStreamException {
		if (this.getDataType() != TYPE_Float)
			throw new InvalidDataStreamException("Trying to read float data from "+types[this.getDataType()]+".");
		return this.data.getConverter().getVariableDataFloat(this.variableIndex);
	}
	
	public int[] getIntData() throws InvalidDataStreamException {
		if (this.getDataType() != TYPE_UnSignedInt32 && this.getDataType() != TYPE_SignedInt32 && this.getDataType() != TYPE_BitSet32)
			throw new InvalidDataStreamException("Trying to read int data from "+types[this.getDataType()]+".");
		return this.data.getConverter().getVariableDataInt(this.variableIndex);
	}
	
	public double[] getDoubleData() throws InvalidDataStreamException {
		if (this.getDataType() != TYPE_Double)
			throw new InvalidDataStreamException("Trying to read double data from "+types[this.getDataType()]+".");
		return this.data.getConverter().getVariableDataDouble(this.variableIndex);
	}
	
	public long[] getLongData() throws InvalidDataStreamException {
		if (this.getDataType() != TYPE_UnSignedInt64 && this.getDataType() != TYPE_SignedInt64 && this.getDataType() != TYPE_BitSet64)
			throw new InvalidDataStreamException("Trying to read boolean data from "+types[this.getDataType()]+".");
		return this.data.getConverter().getVariableDataLong(this.variableIndex);
	}
	
	// TODO: generic way of returning data
	public <T> T getGenericData() {
		final GInsDataKafkaConverter conv = this.data.getConverter();
		switch (variableType) {
		
			// floats: 4 bytes
			case 8:
				return (T)(float[])conv.getVariableDataFloat(this.variableIndex);
				
			// bytes
			case 2:
			case 3:
			case 9:
				return (T)(byte[])conv.getVariableDataByte(this.variableIndex);
		}
		
		return null;
	}
	

	@JsonProperty("DataDirection")
	public int dataDirection;

	@JsonProperty("DataType")
	public int dataType;

	@JsonProperty("FieldLength")
	public int fieldLength;

	@JsonProperty("ID")
	public String id;

	@JsonProperty("Name")
	public String name;

	@JsonProperty("Precision")
	public int precision;

	@JsonProperty("Unit")
	public String unit;

	@JsonProperty("VariableType")
	public int variableType;


	public int getDataDirection() {
		return dataDirection;
	}

	public void setDataDirection(int dataDirection) {
		this.dataDirection = dataDirection;
	}

	public int getDataType() {
		return dataType;
	}

	public void setDataType(int dataType) {
		this.dataType = dataType;
	}

	public int getFieldLength() {
		return fieldLength;
	}

	public void setFieldLength(int fieldLength) {
		this.fieldLength = fieldLength;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getPrecision() {
		return precision;
	}

	public void setPrecision(int precision) {
		this.precision = precision;
	}

	public String getUnit() {
		return unit;
	}

	public void setUnit(String unit) {
		this.unit = unit;
	}

	public int getVariableType() {
		return variableType;
	}

	public void setVariableType(int variableType) {
		this.variableType = variableType;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + dataDirection;
		result = prime * result + dataType;
		result = prime * result + fieldLength;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + precision;
		result = prime * result + ((unit == null) ? 0 : unit.hashCode());
		result = prime * result + variableType;
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
		VariableHeader other = (VariableHeader) obj;
		if (dataDirection != other.dataDirection)
			return false;
		if (dataType != other.dataType)
			return false;
		if (fieldLength != other.fieldLength)
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (precision != other.precision)
			return false;
		if (unit == null) {
			if (other.unit != null)
				return false;
		} else if (!unit.equals(other.unit))
			return false;
		if (variableType != other.variableType)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "TKafkaVariableHeader [dataDirection=" + dataDirection + ", dataType=" + dataType + ", fieldLength="
				+ fieldLength + ", id=" + id + ", name=" + name + ", precision=" + precision + ", unit=" + unit
				+ ", variableType=" + variableType + "]";
	}
	
}
