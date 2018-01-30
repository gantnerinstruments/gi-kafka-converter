package gi.kafka.model.messages;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Iterator;

import org.msgpack.core.MessageUnpacker;

import com.fasterxml.jackson.annotation.JsonProperty;

import gi.kafka.jni.GInsDataKafkaConverter;
import gi.kafka.model.GInsData;
import gi.kafka.model.InvalidDataStreamException;

public class VariableHeader {

	/*public enum DataType {
		No (0),
		Boolean (1),
		SignedInt8 (2),
		UnSignedInt8 (3),
		SignedInt16 (4),
		UnSignedInt16 (5),
		SignedInt32 (6),
		UnSignedInt32 (7),
		Float (8),
		BitSet8 (9),
		BitSet16 (10),
		BitSet32 (11),
		Double (12),
		SignedInt64 (13),
		UnSignedInt64 (14),
		BitSet64 (15);
		
		private int type;
		DataType(int type) {
			this.type = type;
		}
		
		public int type() {
			return this.type;
		}
	}*/
	
	// dataType
	public static final int DATA_TYPE_No = 0;
	public static final int DATA_TYPE_Boolean = 1;
	public static final int DATA_TYPE_SignedInt8 = 2;
	public static final int DATA_TYPE_UnSignedInt8 = 3;
	public static final int DATA_TYPE_SignedInt16 = 4;
	public static final int DATA_TYPE_UnSignedInt16 = 5;
	public static final int DATA_TYPE_SignedInt32 = 6;
	public static final int DATA_TYPE_UnSignedInt32 = 7;
	public static final int DATA_TYPE_Float = 8;
	public static final int DATA_TYPE_BitSet8 = 9;
	public static final int DATA_TYPE_BitSet16 = 10;
	public static final int DATA_TYPE_BitSet32 = 11;
	public static final int DATA_TYPE_Double = 12;
	public static final int DATA_TYPE_SignedInt64 = 13;
	public static final int DATA_TYPE_UnSignedInt64 = 14;
	public static final int DATA_TYPE_BitSet64 = 15;
	public static final String[] dataTypes = {
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
	
	// variableType
	public static final int VARIABLE_TYPE_Empty = 0;
	public static final int VARIABLE_TYPE_AnalogInput = 1;
	public static final int VARIABLE_TYPE_Arithmetic = 2;
	public static final int VARIABLE_TYPE_DigitalOutput = 3;
	public static final int VARIABLE_TYPE_DigitalInput = 4;
	public static final int VARIABLE_TYPE_SetPoint = 5;
	public static final int VARIABLE_TYPE_Alarm = 6;
	public static final int VARIABLE_TYPE_BitsetOutput = 7;
	public static final int VARIABLE_TYPE_BitsetInput = 8;
	public static final int VARIABLE_TYPE_PIDController = 9;
	public static final int VARIABLE_TYPE_AnalogOutput = 10;
	public static final int VARIABLE_TYPE_SignalConditioning = 11;
	public static final int VARIABLE_TYPE_RemoteInput = 12;
	public static final int VARIABLE_TYPE_Reference = 13;
	public static final String[] variableTypes = {
		"empty",
		"analog input",
		"arithmetic",
		"digital output",
		"digital input",
		"set point",
		"alarm",
		"bitset output",
		"bitset input",
		"pid controller",
		"analog output",
		"signal conditioning",
		"remote input",
		"reference",
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
		if (this.getDataType() != DATA_TYPE_Boolean)
			throw new InvalidDataStreamException("Trying to read boolean data from "+dataTypes[this.getDataType()]+".");
		return this.data.getConverter().getVariableDataBoolean(this.variableIndex);
	}
	
	public byte[] getRawData() {
		return this.data.getConverter().getVariableDataRaw(this.variableIndex);
	}
	
	public byte[] getByteData() throws InvalidDataStreamException {
		if (this.getDataType() != DATA_TYPE_UnSignedInt8 && this.getDataType() != DATA_TYPE_SignedInt8 && this.getDataType() != DATA_TYPE_BitSet8)
			throw new InvalidDataStreamException("Trying to read byte data from "+dataTypes[this.getDataType()]+".");
		return this.data.getConverter().getVariableDataByte(this.variableIndex);
	}

	public short[] getShortData() throws InvalidDataStreamException {
		if (this.getDataType() != DATA_TYPE_UnSignedInt16 && this.getDataType() != DATA_TYPE_SignedInt16 && this.getDataType() != DATA_TYPE_BitSet16)
			throw new InvalidDataStreamException("Trying to read short data from "+dataTypes[this.getDataType()]+".");
		return this.data.getConverter().getVariableDataShort(this.variableIndex);
	}
	
	public float[] getFloatData() throws InvalidDataStreamException {
		if (this.getDataType() != DATA_TYPE_Float)
			throw new InvalidDataStreamException("Trying to read float data from "+dataTypes[this.getDataType()]+".");
		return this.data.getConverter().getVariableDataFloat(this.variableIndex);
	}
	
	public int[] getIntData() throws InvalidDataStreamException {
		if (this.getDataType() != DATA_TYPE_UnSignedInt32 && this.getDataType() != DATA_TYPE_SignedInt32 && this.getDataType() != DATA_TYPE_BitSet32)
			throw new InvalidDataStreamException("Trying to read int data from "+dataTypes[this.getDataType()]+".");
		return this.data.getConverter().getVariableDataInt(this.variableIndex);
	}
	
	public double[] getDoubleData() throws InvalidDataStreamException {
		if (this.getDataType() != DATA_TYPE_Double)
			throw new InvalidDataStreamException("Trying to read double data from "+dataTypes[this.getDataType()]+".");
		return this.data.getConverter().getVariableDataDouble(this.variableIndex);
	}
	
	public long[] getLongData() throws InvalidDataStreamException {
		if (this.getDataType() != DATA_TYPE_UnSignedInt64 && this.getDataType() != DATA_TYPE_SignedInt64 && this.getDataType() != DATA_TYPE_BitSet64)
			throw new InvalidDataStreamException("Trying to read long data from "+dataTypes[this.getDataType()]+".");
		return this.data.getConverter().getVariableDataLong(this.variableIndex);
	}
	
	/**
	 * Use some-what generic way of requesting data. It's not recommended to use that during production,
	 * because all values are copied into a Number array, which is then returned. However it's great
	 * for testing purposes, because you can get every data array by using 1 method.
	 * 
	 * @return
	 */
	public Number[] getGenericData() {
		final GInsDataKafkaConverter conv = this.data.getConverter();
		final Number[] res;
		
		//System.out.println("type: "+dataType);
		switch (this.dataType) {
		
			case DATA_TYPE_Boolean:
				final boolean[] booldata = conv.getVariableDataBoolean(this.variableIndex);
				res = new Number[booldata.length];
				for (int i = 0; i < res.length; i++)
					res[i] = booldata[i] ? 1 : 0;
				break;
				//return (T) (boolean[])conv.getVariableDataBoolean(this.variableIndex);
				
			case DATA_TYPE_Float:
				final float[] fdata = conv.getVariableDataFloat(this.variableIndex);
				res = new Number[fdata.length];
				for (int i = 0; i < res.length; i++)
					res[i] = fdata[i];
				break;
				
			case DATA_TYPE_Double:
				final double[] ddata = conv.getVariableDataDouble(this.variableIndex);
				res = new Number[ddata.length];
				for (int i = 0; i < res.length; i++)
					res[i] = ddata[i];
				break;
		
			// 1 byte
			case DATA_TYPE_UnSignedInt8:
			case DATA_TYPE_SignedInt8:
			case DATA_TYPE_BitSet8:
				final byte[] bdata = conv.getVariableDataByte(this.variableIndex);
				res = new Number[bdata.length];
				for (int i = 0; i < res.length; i++)
					res[i] = bdata[i];
				break;
				
			// 2 bytes
			case DATA_TYPE_UnSignedInt16:
			case DATA_TYPE_SignedInt16:
			case DATA_TYPE_BitSet16:
				final short[] sdata = conv.getVariableDataShort(this.variableIndex);
				res = new Number[sdata.length];
				for (int i = 0; i < res.length; i++)
					res[i] = sdata[i];
				break;
				
			// 4 bytes
			case DATA_TYPE_UnSignedInt32:
			case DATA_TYPE_SignedInt32:
			case DATA_TYPE_BitSet32:
				final int[] idata = conv.getVariableDataInt(this.variableIndex);
				res = new Number[idata.length];
				for (int i = 0; i < res.length; i++)
					res[i] = idata[i];
				break;
				
			// 8 bytes
			case DATA_TYPE_UnSignedInt64:
			case DATA_TYPE_SignedInt64:
			case DATA_TYPE_BitSet64:
				final long[] ldata = conv.getVariableDataLong(this.variableIndex);
				res = new Number[ldata.length];
				for (int i = 0; i < res.length; i++)
					res[i] = ldata[i];
				break;
				
			// type not found or unknown, return raw
			default:
				final byte[] rdata = conv.getVariableDataRaw(this.variableIndex);
				res = new Number[rdata.length];
				for (int i = 0; i < res.length; i++)
					res[i] = rdata[i];
				break;
		}
		
		return res;
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
