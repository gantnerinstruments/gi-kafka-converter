package gi.kafka.jni;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

public class GInsDataKafkaConverter {
	
	
	private static String getAppendix() {
		String arch = System.getProperty("sun.arch.data.model");
		if (arch.equalsIgnoreCase("64"))
			return "_64";
		else
			return "_32";
	}
	private static final String DATA_LIB = "giutility"+getAppendix();
	private static final String CONVERTER_LIB = "GInsData_Kafka_Converter"+getAppendix();

	
	private boolean linked = false;
	private void setupLinker() {
		if (linked)
			return;
		
		// debugging output
		//String arch = System.getProperty("sun.arch.data.model");
		//String libPath = System.getProperty("java.library.path");
		//System.out.println("java.library.path=" + libPath+", sun.arch.data.model="+arch);
		//System.out.println("Working Directory = " + System.getProperty("user.dir"));

		// unpack libraries, store them in temp folder and load
		loadFromJar();

	}

	/**
	 * Load native libraries out of jar bundle.
	 * Stores them in temp folder.
	 */
	private void loadFromJar() {
		// we need to put both DLLs to temp dir
		// String path = "GIns_" + new Date().getTime();
		loadLib("", DATA_LIB, true);
		loadLib("", CONVERTER_LIB, true);
		linked = true;
	}

	/**
	 * Puts library to temp dir and loads to memory
	 */
	private void loadLib(String path, String name, boolean link) {
		final String ending = ".dll";
		
		final String resourceName = name + ending;
		final String writeName = name.replace("_32", "").replace("_64", "") + ending;
		try {
			// have to use a stream
			final InputStream in = GInsDataKafkaConverter.class.getResourceAsStream("/"+resourceName);
			
			// always write to different location
			File fileOut = new File(System.getProperty("java.io.tmpdir") + "/" + path + writeName);
			OutputStream out = FileUtils.openOutputStream(fileOut);

			System.out.println("Writing "+resourceName+" to: " + fileOut.getAbsolutePath());
			
			IOUtils.copy(in, out);
			in.close();
			out.close();
			if (link)
				System.load(fileOut.toString());
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to load required DLL", e);
		}
	}
	
	/**
	 * Creates a link to native GIKafkaConverter c++ library
	 * @param data input byte[] data array
	 */
	public GInsDataKafkaConverter(byte[] data) {
		setupLinker();
		final boolean worked = this.load(data, data.length);
		if (!worked)
			throw new RuntimeException("[GinsDataConverter]: Could not load data.");
	}

	public native boolean load(byte[] data, int length);
	public native byte[] getMeta();
	public native void free();
	
	public native double[] getVariableDataDouble(int varIdx);
	public native boolean[] getVariableDataBoolean(int varIdx);
	public native byte[] getVariableDataByte(int varIdx);
	public native char[] getVariableDataChar(int varIdx);
	public native short[] getVariableDataShort(int varIdx);
	public native int[] getVariableDataInt(int varIdx);
	public native long[] getVariableDataLong(int varIdx);
	public native float[] getVariableDataFloat(int varIdx);
}
