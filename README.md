# gi-kafka-converter

## Data Structure


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
