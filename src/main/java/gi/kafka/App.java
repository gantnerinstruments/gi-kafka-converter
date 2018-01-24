package gi.kafka;

import java.io.IOException;
import java.util.Arrays;

import gi.kafka.model.GinsData;
import gi.kafka.model.messages.MetaHeader;
import gi.kafka.model.messages.VariableHeader;


public class App {
	
	public static void main(String[] args) {
		try {
			if (args.length > 0) {
				final GinsData data = new GinsData(args[0]);
				
				final MetaHeader header = data.getMeta().getPacketHeader();
				System.out.println("Header: "+header);
				
				int idx = 0;
				for (VariableHeader var : header.getVariables()) {
					System.out.println("Variables["+var.getName()+", "+(idx++)+"]");
					System.out.println(Arrays.toString(var.getLongData()));
				}
				
			} else {
				System.out.println("No input file specified!");
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
