package gi.kafka;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;

import gi.kafka.model.GinsDeserializer;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class AppTest extends TestCase {
	
	public AppTest(String testName) {
		super(testName);
	}

	
	MockConsumer<String, String> consumer;

	@Before
	public void setUp() {
	    consumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
	}
	
	public static Test suite() {
		return new TestSuite(AppTest.class);
	}



	// TODO
	/*public void testKafkaConsumer() {
	    // This is YOUR consumer object
	    GinsDeserializer myTestConsumer = new GinsDeserializer();
	    // Inject the MockConsumer into your consumer
	    // instead of using a KafkaConsumer
	    myTestConsumer.consumer = consumer;

	    consumer.assign(Arrays.asList(new TopicPartition("my_topic", 0)));

	    HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
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
