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
	private GInsData data;

	@Before
	public void setUp() throws IOException {
		consumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
		data = new GInsData("./res/test1.dat");
	}

	public static Test suite() {
		return new TestSuite(GInsDataTest.class);
	}

	public void testMeta() {
		final GInsDataMetaModel meta = data.getMeta();
		
		assertTrue(meta.getVersion().equals("1.0"));
		assertTrue(meta.getDataCount() == 200);
		assertTrue(meta.getOffsetNS() == 30000128L);
		assertTrue(meta.getSampleRate() == 100.0);
		assertTrue(meta.getVariables().size() == 2);
		
		//System.out.println("meta: "+meta.getPacketHeader());
	}
	
	public void testVariables() throws InvalidDataStreamException {
		final List<VariableHeader> vars = data.getVariables();
		for (VariableHeader var : vars) {
			assertTrue(var.getDataType() == 14);
			assertTrue(var.fieldLength == 8);
			assertTrue(var.precision == 3);
			
			//System.out.println("data: "+Arrays.toString(var.getLongData()));
		}
		
		// verify data array 1
		final long[] data1 = new long[] { -431602080000L, -9223372036854775808L, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		assertTrue(Arrays.equals(data1, vars.get(0).getLongData()));
		
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
		assertTrue(Arrays.equals(data2, vars.get(1).getLongData()));
	}
	
	public void testDataTypes() {
		final VariableHeader var = data.getVariable(0);
		
		// expecting long data here, check if others fail properly
		
		try {
			var.getBooleanData();
			fail("Expected "+InvalidDataStreamException.class+".");
		} catch (Exception e) { }

		try {
			var.getShortData();
			fail("Expected "+InvalidDataStreamException.class+".");
		} catch (Exception e) { }

		try {
			var.getIntData();
			fail("Expected "+InvalidDataStreamException.class+".");
		} catch (Exception e) { }

		try {
			var.getFloatData();
			fail("Expected "+InvalidDataStreamException.class+".");
		} catch (Exception e) { }

		try {
			var.getDoubleData();
			fail("Expected "+InvalidDataStreamException.class+".");
		} catch (Exception e) { }
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
