package gi.kafka.jni;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import gi.kafka.util.OSDetector;

public class GInsDataKafkaConverter {
	
	private static String getDataLibName() {
		return OSDetector.isWindows() ? "giutility" : "libGInsData";
	}
	private static String getConverterLibName() {
		return OSDetector.isWindows() ? "GInsData_Kafka_Converter" : "libGInsData_Kafka_Converter";
	}
	
	private static final String DATA_LIB = getDataLibName();
	private static final String CONVERTER_LIB = getConverterLibName();

	
	private boolean linked = false;
	private void setupLinker() {
		if (linked)
			return;
		
		try {
			System.loadLibrary(DATA_LIB);
			System.loadLibrary(CONVERTER_LIB);
		} catch (UnsatisfiedLinkError e) {
			// libraries are not in classpath or library path
			// unpack and load them out of jar file
		}
		
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

	private void loadLib(String path, String name, boolean link) {
		final String ending = OSDetector.isWindows() ? ".dll" : ".so";
		
		final String resourceName = name + ending;
		final String writeName = name.replace("_x86", "").replace("_x64", "") + ending;
		final String outDirectory = System.getProperty("java.io.tmpdir") + "/" + path + writeName;
		final File fileOut = new File(outDirectory);
		
		try {
			if (!fileOut.exists() || fileOut.length() <= 0 || true) {
				final InputStream in = GInsDataKafkaConverter.class.getResourceAsStream("/"+resourceName);
				
				// write to temp directory
				final OutputStream out = FileUtils.openOutputStream(fileOut);
				System.out.println("[GinsDataKafkaConverter]: writing "+resourceName+" to: " + fileOut.getAbsolutePath());
				System.out.println("In: "+in+", Out: "+out);
				
				// copy files
				IOUtils.copy(in, out);
				in.close();
				out.close();
			}
			
			// library required for explicit linking?
			if (link)
				System.load(fileOut.toString());
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("[GinsDataKafkaConverter]: failed to load required libraries", e);
		}
	}
	
	/**
	 * Creates a link to native GIKafkaConverter c++ library
	 * @param data input byte[] data array
	 */
	public GInsDataKafkaConverter(final byte[] data) {
		setupLinker();
		final boolean worked = this.load(data, data.length);
		if (!worked)
			throw new RuntimeException("[GinsDataConverter]: Could not load data.");
	}

	public native boolean load(byte[] data, int length);
	public native byte[] getMeta();
	public native void free();

	public native byte[] getVariableDataRaw(int varIdx);
	public native double[] getVariableDataDouble(int varIdx);
	public native boolean[] getVariableDataBoolean(int varIdx);
	public native byte[] getVariableDataByte(int varIdx);
	public native char[] getVariableDataChar(int varIdx);
	public native short[] getVariableDataShort(int varIdx);
	public native int[] getVariableDataInt(int varIdx);
	public native long[] getVariableDataLong(int varIdx);
	public native float[] getVariableDataFloat(int varIdx);
}
