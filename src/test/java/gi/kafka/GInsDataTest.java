package gi.kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import gi.kafka.model.GInsData;
import gi.kafka.model.GInsDataMetaModel;
import gi.kafka.model.InvalidDataStreamException;
import gi.kafka.model.messages.VariableHeader;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class GInsDataTest extends TestCase {

	private final MockConsumer<String, String> consumer;
	private final GInsData dataSet1;
	private final GInsData dataSet2;

	public GInsDataTest(String testName) throws IOException {
		super(testName);
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
		final long[] data1 = new long[] { 1091924132L, 1087834685L, 1084374385L, 1077055324L, 1086681252L, 1074203197L,
				1074622628L, 1091976561L, 1087163597L, 1080662426L, 1081459343L, 1076384236L, 1091389358L, 1085422961L,
				1092165304L, 1069966950L, 1091368387L, 1077726413L, 1087771771L, 1075000115L, 1090162524L, 1076845609L,
				1081081856L, 1083346780L, 1078858875L, 1083829125L, 1091253043L, 1087499141L, 1077810299L, 1087331369L,
				1084290499L, 1091882189L, 1077936128L, 1090183496L, 1082528891L, 1075251773L, 1057635697L, 1065604874L,
				1080704369L, 1080536596L, 1055286886L, 1079907451L, 1085842391L, 1090550497L, 1087121654L, 1074077368L,
				1089575322L, 1078103900L, 1082654720L, 1044549468L, 1079697736L, 1088023429L, 1088778404L, 1083871068L,
				1090739241L, 1088296059L, 1091106243L, 1060152279L, 1091777331L, 1080284938L, 1075503432L, 1087457198L,
				1077390868L, 1087268454L, 1086953882L, 1079613850L, 1086219878L, 1091777331L, 1092448420L, 1092563763L,
				1089596293L, 1092186276L, 1091473244L, 1084122726L, 1080578540L, 1091085271L, 1065353216L, 1068960317L,
				1081543229L, 1091661988L, 1090959442L, 1060991140L, 1086723195L, 1076845609L, 1071476900L, 1087813714L,
				1090529526L, 1084856730L, 1092500849L, 1091777331L, 1091326444L, 1078900818L, 1088212173L, 1083451638L,
				1085842391L, 1084730900L, 1079865508L, 1090980413L, 1092490363L, 1063843267L };
		System.out.println("data1: " + Arrays.toString(vars.get(0).getGenericData()));
		// assertTrue(Arrays.equals(data1L, vars.get(0).getLongData()));

		// verify data array 2
		final long[] data2 = new long[] { 4615198826136736891L, 4616898934996069253L, 4621289944632755487L,
				4599075939470750515L, 4620423001704486666L, 4615896884078979318L, 4620636922686786765L,
				4616887675997000827L, 4619263324800438764L, 4619195770806028206L, 4620698847181663109L,
				4620231598720323420L, 4613690120261567775L, 4617754618925269647L, 4619533540778080993L,
				4615378970121831711L, 4621109800647660667L, 4612924508324914790L, 4619871310750133780L,
				4618824223836770140L, 4607407598781385933L, 4611415802449745674L, 4609073930643513016L,
				4617338035959737876L, 4618948072826522829L, 4611956234405030134L, 4619589835773423124L,
				4616471093031469056L, 4617630769935516959L, 4619612353771559977L, 4613600048269020365L,
				4621385646124837110L, 4606371770867090719L, 4616977747989548237L, 4620991581157442191L,
				4617360553957874729L, 4614005372235483709L, 4619297101797644042L, 4610560118520545280L,
				4607362562785112228L, 4620873361667223716L, 4611280694460924559L, 4619657389767833682L,
				4614545804190768169L, 4617214186969985188L, 4601237667291888353L, 4617056560983027220L,
				4619860051751065354L, 4611956234405030134L, 4607227454796291113L, 4621154836643934372L,
				4617709582928995942L, 4617247963967190467L, 4621458829618781880L, 4612721846341683118L,
				4617214186969985188L, 4618925554828385976L, 4596373779694328218L, 4617495661946695844L,
				4616437316034263777L, 4619038144819070239L, 4620265375717528699L, 4608758678669597082L,
				4614410696201947054L, 4618058611900117156L, 4612451630364040888L, 4615559114106926531L,
				4621081653149989601L, 4618846741834906993L, 4617146632975574630L, 4621441941120179241L,
				4613442422282062397L, 4612496666360314593L, 4606281698874543309L, 4621498236115521372L,
				4621661491602013553L, 4612271486378946068L, 4621605196606671421L, 4620710106180731535L,
				4617326776960669450L, 4604840546993784750L, 4619882569749202207L, 4618407640871238369L,
				4621515124614124012L, 4615604150103200236L, 4616504870028674335L, 4614320624209399644L,
				4610425010531724165L, 4621554531110863503L, 4620867732167689503L, 4611866162412482724L,
				4616887675997000827L, 4616583683022153318L, 4616144582058484695L, 4614838538166547251L,
				4619060662817207091L, 4621644603103410913L, 4621695268599218831L, 4614973646155368366L,
				4584304132692975288L };
		System.out.println("data2: " + Arrays.toString(vars.get(1).getGenericData()));
		// assertTrue(Arrays.equals(data2, vars.get(1).getLongData()));
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
		System.out.println("raw1: "+Arrays.toString(rdata1));
		
		final byte[] rdata2 = dataSet2.getVariable(0).getRawData();
		System.out.println("raw2: "+Arrays.toString(rdata2));
		
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
