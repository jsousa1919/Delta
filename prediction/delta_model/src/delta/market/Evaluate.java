package delta.market;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;

import org.encog.neural.data.NeuralData;
import org.encog.neural.data.NeuralDataPair;
import org.encog.neural.data.buffer.BufferedNeuralDataSet;
import org.encog.neural.networks.BasicNetwork;
import org.encog.normalize.DataNormalization;
import org.encog.normalize.output.nominal.OutputEquilateral;
import org.encog.persist.EncogPersistedCollection;
import org.encog.util.csv.ReadCSV;
import org.encog.util.simple.EncogUtility;

import au.com.bytecode.opencsv.CSVWriter;

public class Evaluate {
	public BasicNetwork loadNetwork() {
		File file = Config.NETWORK_FILE;

		if (!file.exists()) {
			System.out.println("Can't read file: " + file.getAbsolutePath());
			return null;
		}

		EncogPersistedCollection encog = new EncogPersistedCollection(file);
		BasicNetwork network = (BasicNetwork) encog
				.find(Config.TRAINED_NETWORK_NAME);

		if (network == null) {
			System.out.println("Can't find network resource: "
					+ Config.TRAINED_NETWORK_NAME);
			return null;
		}

		return network;
	}

	public DataNormalization loadNormalization() {
		File file = Config.NETWORK_FILE;

		EncogPersistedCollection encog = new EncogPersistedCollection(file);

		DataNormalization norm = (DataNormalization) encog
				.find(Config.NORMALIZATION_NAME);
		if (norm == null) {
			System.out.println("Can't find normalization resource: "
					+ Config.NORMALIZATION_NAME);
			return null;
		}

		return norm;
	}
	
	public void evaluate() {
		BasicNetwork network = loadNetwork();
		DataNormalization norm = loadNormalization();
		
		EncogUtility.convertCSV2Binary(Config.NORMALIZED_TEST_FILE, Config.BINARY_FILE, Config.INPUT_COUNT - 1, 1, false);
		BufferedNeuralDataSet data = new BufferedNeuralDataSet(Config.BINARY_FILE);
		
		int correct = 0;
		int count = 0;

		DecimalFormat format = new DecimalFormat("#0.0000");
		try {
			CSVWriter writer = new CSVWriter(new FileWriter(Config.BASE_DIR + Config.EVALUATE_OUTPUT), ',');
			String[] output = new String[3];
			writer.writeNext("Actual,Predict,Difference".split(","));
			for (NeuralDataPair pair : data) {
				NeuralData input = pair.getInput();
				//System.out.println(input);
				NeuralData actualData = pair.getIdeal();
				NeuralData predictData = network.compute(input);
				//System.out.println(actualData + " " + predictData);
				double actual = actualData.getData(0);
				double predict = predictData.getData(0);
				double diff = Math.abs(predict - actual);
				//System.out.println(actual + " " + predict);
				count++;
				output[0] = actual + "";
				output[1] = predict + "";
				output[2] = diff + "";
				writer.writeNext(output);
				//System.out.println("Day " + count + ":actual="
				//		+ format.format(actual) + ",predict=" + format.format(predict) + ",diff=" + diff);

			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		data.close();
		//System.out.println("total is : " + count);
	}
}
