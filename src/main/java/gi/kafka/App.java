package gi.kafka;

import java.io.IOException;
import java.util.Arrays;

import gi.kafka.model.GinsData;
import gi.kafka.model.messages.MetaHeader;

/**
 * Hello world!
 *
 */
public class App {
	
	public static void main(String[] args) {
		try {
			GinsData data = new GinsData("./res/test1.dat");
			
			MetaHeader header = data.getMeta().getPacketHeader();
			System.out.println("header: "+header);
			
			final float[] vars = header.getVariables().get(0).getFloatData();
			System.out.println("vars: "+Arrays.toString(vars));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
