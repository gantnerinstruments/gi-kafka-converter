package gi.kafka;

import java.io.IOException;
import java.util.Arrays;

import gi.kafka.model.GInsData;
import gi.kafka.model.InvalidDataStreamException;
import gi.kafka.model.messages.MetaHeader;
import gi.kafka.model.messages.VariableHeader;


public class Main {
	
	public static void main(String[] args) {
		try {
			if (args.length > 0) {
				final GInsData data = new GInsData(args[0]);
				
				final MetaHeader header = data.getMeta().getPacketHeader();
				System.out.println("Header: "+header);
				
				int idx = 0;
				//System.out.println("Vars: "+header.getVariables().size());
				for (VariableHeader var : header.getVariables()) {
					System.out.println("Variables["+var.getName()+", "+(idx++)+"]");
					System.out.println(Arrays.toString(var.getGenericData()));
				}
				
			} else {
				System.out.println("No input file specified!");
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
