package delta.market;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.encog.neural.data.market.TickerSymbol;

import au.com.bytecode.opencsv.CSVWriter;

/*
 * 
 * For holding necessary variables
 * 
 */

public class Config {

	public static String BASE_DIR;// = "/home/tom/financial_data/encog_data/";
	public static String DATA_DIR;// = "/home/tom/financial_data/encog_data/";
	public static String START_DIR;
	//public static String trial_dir;
	public static final String FILENAME = "marketdata.eg";
	//public static final String MARKET_NETWORK = "market-network";
	//public static final String MARKET_TRAIN = "market-train";
	public static final int TRAINING_MINUTES = 5;
	public static final int HIDDEN1_COUNT = 20;
	public static final int HIDDEN2_COUNT = 0;
	public static final int INPUT_COUNT = 12;
	public static int NETWORK_TYPE = 0; //0 = feedforward, 1 = elman, 2 = jordan
	/*
	public static final Calendar TRAIN_BEGIN = new GregorianCalendar(1990, 0, 1);
	public static final Calendar TRAIN_END = new GregorianCalendar(2005, 12, 31);
	public static final Calendar TEST_BEGIN = new GregorianCalendar(2006, 0, 1);
	public static final Calendar TEST_END = new GregorianCalendar(2010, 12, 31);
	*/
	//public static final int INPUT_WINDOW = 10;
	//public static final int PREDICT_WINDOW = 1;
	//public static final TickerSymbol TICKER = new TickerSymbol("AAPL");
	
	//public static final String TRAINING_NAME = "MCD_train.csv";
	//public static final File NORMALIZED_FILE = new File(BASE_DIR, "normalized.csv");
	public static String TRAINING_FILENAME;
	public static File NORMALIZED_FILE;
	public static File TRAINING_FILE; // = new File(BASE_DIR, TRAINING_NAME);
	
	//for test data
	//public static final String TEST_NAME = "MCD_sub.csv";
	//public static final File NORMALIZED_TEST_FILE = new File(BASE_DIR, "test_normalized.csv");
	public static String TEST_FILENAME;
	public static File NORMALIZED_TEST_FILE;
	public static File TEST_FILE; // = new File(BASE_DIR, TRAINING_NAME);
	
	public static String NETWORK_FILENAME;
	public static File NETWORK_FILE; // = new File(BASE_DIR,"elman.eg");
	public static String NORMALIZATION_NAME = "norm"; // = "elman-norm";
	public static File BINARY_FILE; // = new File(BASE_DIR, "normalized.bin");
	public static String TRAINED_NETWORK_NAME = "train_network";// = "elman-network";

	public static String TEST_NETWORK_FILENAME;
	public static File TEST_NETWORK_FILE; // = new File(BASE_DIR,"elman.eg");
	public static String NORMALIZATION_TEST_NAME; // = "elman-test-norm";
	public static File BINARY_TEST_FILE; // = new File(BASE_DIR, "normalized.bin");
	
	public static String EVALUATE_OUTPUT;
	
	public static final int HIDDEN_COUNT = 20;
	
	public static String CONFIG_FILENAME;
	
	public static void setupConfig(){
		TRAINING_FILE = new File(DATA_DIR, TRAINING_FILENAME);
		TEST_FILE= new File(DATA_DIR, TEST_FILENAME);
		NORMALIZED_TEST_FILE = new File(BASE_DIR, "test_normalized.csv");
		NORMALIZED_FILE = new File(BASE_DIR, "normalized.csv");
		NETWORK_FILE = new File(BASE_DIR, NETWORK_FILENAME);
		BINARY_FILE = new File(BASE_DIR, "normalized.bin");
		TEST_NETWORK_FILE = new File(BASE_DIR, NETWORK_FILENAME);
		BINARY_TEST_FILE = new File(BASE_DIR, "test_normalized.bin");
		NORMALIZATION_TEST_NAME = NORMALIZATION_NAME + "-test";
	}
	
	public static void writeInfo(String filename){
		try {
			CONFIG_FILENAME = BASE_DIR + filename;
		    BufferedWriter out = new BufferedWriter(new FileWriter(CONFIG_FILENAME));
		    out.write("Training file: " + TRAINING_FILENAME + "\n");
		    out.write("Test file: " + TEST_FILENAME + "\n");
		    out.write("Encog file: " + NETWORK_FILENAME + "\n");
		    out.write("Network name: " + TRAINED_NETWORK_NAME + "\n");
		    out.write("Normalized name: " + NORMALIZATION_NAME + "\n");
		    out.write("Output name: " + EVALUATE_OUTPUT + "\n");
		    out.close();
		} catch (IOException e) {
			System.out.println("Could not write file\n");
		}
	}
	
	public static void appendConfig(String write_data){
		try {
		    BufferedWriter out = new BufferedWriter(new FileWriter(CONFIG_FILENAME, true));
		    out.write(write_data + "\n");
		    out.close();
		} catch (IOException e) {
			System.out.println("Could not write file\n");
		}
	}
}
