package gi.kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.Before;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import gi.kafka.model.GInsData;
import gi.kafka.model.GInsDataMetaModel;
import gi.kafka.model.InvalidDataStreamException;
import gi.kafka.model.messages.VariableHeader;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class GInsDataTest extends TestCase {

	public GInsDataTest(String testName) {
		super(testName);
	}

	private MockConsumer<String, String> consumer;
	private GInsData dataSet1;
	private GInsData dataSet2;

	@Before
	public void setUp() throws IOException {
		consumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
		dataSet1 = new GInsData("./res/test1.dat");
		dataSet2 = new GInsData("./res/test2_kafka_export.dat");
	}

	public static Test suite() {
		return new TestSuite(GInsDataTest.class);
	}
	
	public void testEnums() {
		assertTrue(VariableHeader.dataTypes.length == 16);
		assertTrue(VariableHeader.variableTypes.length == 14);
	}

	public void testMeta() {
		final GInsDataMetaModel meta1 = dataSet1.getMeta();
		final GInsDataMetaModel meta2 = dataSet2.getMeta();

		System.out.println("meta1: "+meta2.toString());
		assertTrue(meta1.getVersion().equals("1.0"));
		assertTrue(meta1.getDataCount() == 200);
		//assertTrue(meta1.getOffsetNS() == 30000128L);
		assertTrue(meta1.getSampleRate() == 100.0);
		assertTrue(meta1.getVariables().size() == 2);
		
		System.out.println("meta2: "+meta2.toString());
		assertTrue(meta2.getVersion().equals("1.0"));
		assertTrue(meta2.getDataCount() == 1500);
		//assertTrue(meta2.getOffsetNS() == 10000145);
		assertTrue(meta2.getSampleRate() == 100.0);
		assertTrue(meta2.getVariables().size() == 15);
	}
	
	public void testVariables1() throws InvalidDataStreamException {
		final List<VariableHeader> vars = dataSet1.getVariables();
		for (VariableHeader var : vars) {
			assertTrue(var.getDataType() == 14);
			assertTrue(var.fieldLength == 8);
			assertTrue(var.precision == 3);
			
			//System.out.println("data: "+Arrays.toString(var.getLongData()));
		}
		
		// TODO: verify data with correct array
		
		// verify data array 1
		final long[] data1 = new long[] { -431602080000L, -9223372036854775808L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		System.out.println("data1: "+Arrays.toString(vars.get(0).getGenericData()));
		//assertTrue(Arrays.equals(data1, vars.get(0).getLongData()));
		
		// verify data array 2
		final long[] data2 = new long[] { -431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L,
				-431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L,
				-431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L,
				-431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L,
				-431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L,
				-431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L,
				-431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L,
				-431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L,
				-431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L,
				-431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L,
				-431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L,
				-431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L,
				-431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L,
				-431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L,
				-431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L,
				-431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L,
				-431602080000L, -431602080000L, -431602080000L, -431602080000L, -431602080000L };
		System.out.println("data2: "+Arrays.toString(vars.get(1).getGenericData()));
		//assertTrue(Arrays.equals(data2, vars.get(1).getLongData()));
	}
	
	public void testDataTypes1() throws InvalidDataStreamException {
		final VariableHeader var = dataSet1.getVariable(0);
		
		// expecting long data here, check if others fail properly
		var.getLongData();
		
		try {
			var.getBooleanData();
			fail("Expected "+InvalidDataStreamException.class+".");
		} catch (InvalidDataStreamException e) { }

		try {
			var.getByteData();
			fail("Expected "+InvalidDataStreamException.class+".");
		} catch (InvalidDataStreamException e) { }
		
		try {
			var.getShortData();
			fail("Expected "+InvalidDataStreamException.class+".");
		} catch (InvalidDataStreamException e) { }

		try {
			var.getIntData();
			fail("Expected "+InvalidDataStreamException.class+".");
		} catch (InvalidDataStreamException e) { }

		try {
			var.getFloatData();
			fail("Expected "+InvalidDataStreamException.class+".");
		} catch (InvalidDataStreamException e) { }

		try {
			var.getDoubleData();
			fail("Expected "+InvalidDataStreamException.class+".");
		} catch (InvalidDataStreamException e) { }
	}
	
	public void testGenericDataRetriever() {

		// this can be any data type
		final Number[] gdata1 = dataSet1.getVariable(0).getGenericData();
		final Number[] gdata2 = dataSet1.getVariable(1).getGenericData();
	}
	
	public void testRawDataRetriever() {

		// raw data works
		final byte[] rdata1 = dataSet1.getVariable(0).getRawData();
		final byte[] rdata2 = dataSet2.getVariable(0).getRawData();
	}
	
	public void testDataTypes2() throws InvalidDataStreamException {
		final List<VariableHeader> vars = dataSet2.getVariables();
		//System.out.println("Meta: "+dataSet2.getMeta());
		
		int bools = 0, floats = 0, doubles = 0, bytes = 0, shorts = 0, ints = 0, longs = 0;
		boolean[] dataTypes = new boolean[VariableHeader.dataTypes.length];
		
		for (VariableHeader var : vars) {
			dataTypes[var.getDataType()] = true;
			
			switch (var.getDataType()) {
			
				case VariableHeader.DATA_TYPE_Boolean:
					var.getBooleanData();
					bools++;
					break;
					
				case VariableHeader.DATA_TYPE_Float:
					var.getFloatData();
					floats++;
					break;
					
				case VariableHeader.DATA_TYPE_Double:
					var.getDoubleData();
					doubles++;
					break;
			
				// 1 byte
				case VariableHeader.DATA_TYPE_UnSignedInt8:
				case VariableHeader.DATA_TYPE_SignedInt8:
				case VariableHeader.DATA_TYPE_BitSet8:
					var.getByteData();
					bytes++;
					break;
					
				// 2 bytes
				case VariableHeader.DATA_TYPE_UnSignedInt16:
				case VariableHeader.DATA_TYPE_SignedInt16:
				case VariableHeader.DATA_TYPE_BitSet16:
					var.getShortData();
					shorts++;
					break;
					
				// 4 bytes
				case VariableHeader.DATA_TYPE_UnSignedInt32:
				case VariableHeader.DATA_TYPE_SignedInt32:
				case VariableHeader.DATA_TYPE_BitSet32:
					var.getIntData();
					ints++;
					break;
					
				// 8 bytes
				case VariableHeader.DATA_TYPE_UnSignedInt64:
				case VariableHeader.DATA_TYPE_SignedInt64:
				case VariableHeader.DATA_TYPE_BitSet64:
					var.getLongData();
					longs++;
					break;
					
				// type not found or unknown, return raw
				// this is an error that will be found!
				default:
					var.getRawData();
					throw new InvalidDataStreamException("unknown data stream: "+var.getDataType());
			}
			
			//System.out.println("Header: "+var.toString());
			System.out.println(String.format("%-35s= %s" , "[dataT:"+var.getDataType()+", varT:"+var.getVariableType()+", "+var.getName()+"", Arrays.toString(Arrays.copyOf(var.getGenericData(), 10))));
		}
		
		// test if all data types occur at least once
		// (should work if the above test works too)
		// this just checks that no dataType exists twice
		for (boolean b : dataTypes)
			assertTrue(b);
		
		// check if the amounts are correct
		assertTrue(bools == 1);
		assertTrue(floats == 1);
		assertTrue(doubles == 1);
		assertTrue(bytes == 3);
		assertTrue(shorts == 3);
		assertTrue(ints == 3);
		assertTrue(longs == 3);
	}
	
	// TODO
	/*public void testKafkaConsumer() {

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		final KafkaConsumer<String, Long> consumer1 = new KafkaConsumer<String, Long>(props, new StringDeserializer(), new GInsDeserializer<Long>());
		consumer1.
		
		
		consumer.assign(Arrays.asList(new TopicPartition("my_topic", 0)));

		HashMap<TopicPartition, Long> beginningOffsets = new HashMap<TopicPartition, Long>();
		beginningOffsets.put(new TopicPartition("my_topic", 0), 0L);
		consumer.updateBeginningOffsets(beginningOffsets);

		consumer.addRecord(new ConsumerRecord<String, String>("my_topic", 0, 0L, "mykey", "myvalue0"));
		consumer.addRecord(new ConsumerRecord<String, String>("my_topic", 0, 1L, "mykey", "myvalue1"));
		consumer.addRecord(new ConsumerRecord<String, String>("my_topic", 0, 2L, "mykey", "myvalue2"));
		consumer.addRecord(new ConsumerRecord<String, String>("my_topic", 0, 3L, "mykey", "myvalue3"));
		consumer.addRecord(new ConsumerRecord<String, String>("my_topic", 0, 4L, "mykey", "myvalue4"));

		// This is where you run YOUR consumer's code
		// This code will consume from the Consumer and do your logic on it
		myTestConsumer.consume();

		// This just tests for exceptions
		// Somehow test what happens with the consume()
	}*/
}
