# gi-kafka-converter

## Kafka Entry Structure

| Bytes | Description |
|-|-|
| 1 | Version |
| 2 | Total length of data |
| 2 | Length to MsgPack data |
| n | MsgPack data |
| n | Data |

#### Data Structure
Measured values are appended as binary data variable by variable.

| V_0 Data | V_1 Data | ... | V_n Data |
|-|-|-|-|

The length of one variable block is calculated by ``DataCount / Variables.Length x LengthOf(DataType) `` (see MsgPack header).

Each entry (e.g. V_0 Data) contains all measured data from this variable for all timestamps.

| V_0_t0 | V_0_t1 | ... | V_0_tn | V_1_t0 | V_1_t1 | ... |
|-|-|-|-|-|-|-|

Timstamp of first value: ``t0 = Kafka_Timstamp * 1E6 + Offset_ns``

Timstamps of next values: ``tn = t0 + i * 1E9/SampleRate``, where ``i`` is the index of processed entry.

## MsgPack Header
```c++
struct TKafkaVariableHeader
{
	int DataDirection;
	int DataType;
	int FieldLength;
	std::string ID;
	std::string Name;
	int Precision;
	std::string Unit;
	int VariableType;

	MSGPACK_DEFINE(DataDirection, DataType, FieldLength, ID, Name, Precision, Unit, VariableType);
};

struct TKafkaPacketHeader
{
	std::string Version;
	int DataCount;
	int Offset_ns;
	double SampleRate;
	std::vector<TKafkaVariableHeader> Variables;

	MSGPACK_DEFINE(Version, DataCount, Offset_ns, SampleRate, Variables);
};
```
### Enumerations
#### DataDirection
| Enum | DataDirection |
|------|---------------|
| 0 | Input |
| 1 | Output |
| 2 | Input/Output |
| 3 | Empty |
#### DataType
| Enum | DataType |
|------|----------|
| 0 | No |
| 1 | Boolean |
| 2 | SInt8 |
| 3 | USInt8 |
| 4 | SInt16 |
| 5 | USInt16 |
| 6 | SInt32 |
| 7 | USInt32 |
| 8 | Float |
| 9 | BitSet8 |
| 10 | BitSet16 |
| 11 | BitSet32 |
| 12 | Double |
| 13 | SInt64 |
| 14 | USInt64 |
| 15 | BitSet64 |
#### VariableType
| Enum | VariableType |
|-|-|
| 0 | Empty |
| 1 | AnalogInput |
| 2 | Arithmetic |
| 3 | DigitalOutput |
| 4 | DigitalInput |
| 5 | Setpoint |
| 6 | Alarm |
| 7 | - |
| 8 | - |
| 9 | Controller |
| 10 | AnalogOutput |
| 11 | SignalConditioning |
| 12 | Remote |
| 13 | Reference |
| -1 | Unknown |
