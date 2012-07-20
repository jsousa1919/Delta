package delta.market;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.encog.engine.StatusReportable;
import org.encog.normalize.DataNormalization;
import org.encog.normalize.input.InputField;
import org.encog.normalize.input.InputFieldCSV;
import org.encog.normalize.output.OutputField;
import org.encog.normalize.output.OutputFieldRangeMapped;
import org.encog.normalize.segregate.IntegerBalanceSegregator;
import org.encog.normalize.segregate.index.IndexSampleSegregator;
import org.encog.normalize.target.NormalizationStorageCSV;
import org.encog.persist.EncogPersistedCollection;
/*
 * 
 * Generate the data
 * 
 */

import com.Ostermiller.util.StringTokenizer;

public class GenerateData implements StatusReportable{
	
	public void createNormData(boolean is_train) throws IOException{
		
		DataNormalization norm = genData(is_train);
		EncogPersistedCollection encog;
		if(is_train){
			encog = new EncogPersistedCollection(Config.NETWORK_FILE);
			System.out.println(Config.NORMALIZATION_NAME);
			encog.add(Config.NORMALIZATION_NAME, norm);
		}
		else{
			encog = new EncogPersistedCollection(Config.NETWORK_FILE);
			encog.add(Config.NORMALIZATION_TEST_NAME, norm);
		}
	}
	
	public DataNormalization genData(boolean is_train) throws IOException{
		
		
		DataNormalization norm = new DataNormalization();
		norm.setReport(this);
		if(is_train)
			norm.setTarget(new NormalizationStorageCSV(Config.NORMALIZED_FILE));
		else
			norm.setTarget(new NormalizationStorageCSV(Config.NORMALIZED_TEST_FILE));
		//InputField[] past_prices = new InputField[Config.INPUT_WINDOW];
		int input_size = Config.INPUT_COUNT;
		InputField[] past_prices = new InputField[input_size];
		InputField present_price;
		int i;
		if(is_train){
			for(i = 0; i < input_size; i++)
				norm.addInputField(past_prices[i] = new InputFieldCSV(true,Config.TRAINING_FILE,i+2));
				
			//norm.addInputField(present_price = new InputFieldCSV(true, Config.TRAINING_FILE, i));
			/*norm.addInputField(past_prices[0] = new InputFieldCSV(true, Config.TRAINING_FILE, 2));
			norm.addInputField(past_prices[1] = new InputFieldCSV(true, Config.TRAINING_FILE, 3));
			norm.addInputField(past_prices[2] = new InputFieldCSV(true, Config.TRAINING_FILE, 4));
			norm.addInputField(past_prices[3] = new InputFieldCSV(true, Config.TRAINING_FILE, 5));
			norm.addInputField(past_prices[4] = new InputFieldCSV(true, Config.TRAINING_FILE, 7));
			norm.addInputField(past_prices[5] = new InputFieldCSV(true, Config.TRAINING_FILE, 8));
			norm.addInputField(past_prices[6] = new InputFieldCSV(true, Config.TRAINING_FILE, 9));
			norm.addInputField(past_prices[7] = new InputFieldCSV(true, Config.TRAINING_FILE, 10));
			norm.addInputField(past_prices[8] = new InputFieldCSV(true, Config.TRAINING_FILE, 11));
			norm.addInputField(past_prices[9] = new InputFieldCSV(true, Config.TRAINING_FILE, 12));
			norm.addInputField(past_prices[10] = new InputFieldCSV(true, Config.TRAINING_FILE, 13));
			norm.addInputField(past_prices[11] = new InputFieldCSV(true, Config.TRAINING_FILE, 14));
			norm.addInputField(past_prices[12] = new InputFieldCSV(true, Config.TRAINING_FILE, 15));
			norm.addInputField(past_prices[13] = new InputFieldCSV(true, Config.TRAINING_FILE, 16));*/			
		}
		else{
			for(i = 0; i < input_size; i++)
				norm.addInputField(past_prices[i] = new InputFieldCSV(true,Config.TEST_FILE,i+2));
			//norm.addInputField(present_price = new InputFieldCSV(true, Config.TEST_FILE, i));
			/*norm.addInputField(past_prices[0] = new InputFieldCSV(true, Config.TEST_FILE, 2));
			norm.addInputField(past_prices[1] = new InputFieldCSV(true, Config.TEST_FILE, 3));
			norm.addInputField(past_prices[2] = new InputFieldCSV(true, Config.TEST_FILE, 4));
			norm.addInputField(past_prices[3] = new InputFieldCSV(true, Config.TEST_FILE, 5));
			norm.addInputField(past_prices[4] = new InputFieldCSV(true, Config.TEST_FILE, 7));
			norm.addInputField(past_prices[5] = new InputFieldCSV(true, Config.TEST_FILE, 8));
			norm.addInputField(past_prices[6] = new InputFieldCSV(true, Config.TEST_FILE, 9));
			norm.addInputField(past_prices[7] = new InputFieldCSV(true, Config.TEST_FILE, 10));		
			norm.addInputField(past_prices[8] = new InputFieldCSV(true, Config.TEST_FILE, 11));		
			norm.addInputField(past_prices[9] = new InputFieldCSV(true, Config.TEST_FILE, 12));		
			norm.addInputField(past_prices[10] = new InputFieldCSV(true, Config.TEST_FILE, 13));		
			norm.addInputField(past_prices[11] = new InputFieldCSV(true, Config.TEST_FILE, 14));		
			norm.addInputField(past_prices[12] = new InputFieldCSV(true, Config.TEST_FILE, 15));		
			norm.addInputField(past_prices[13] = new InputFieldCSV(true, Config.TEST_FILE, 16));*/		
		}
		
		
		//File file = new File(Config.TRAINING_FILE);
		/*
		BufferedReader bufRdr  = new BufferedReader(new FileReader(Config.TRAINING_FILE));
		String line = null;
		int row = 0;
		int col = 0;
		 
		//read each line of text file
		while((line = bufRdr.readLine()) != null)
		{
			StringTokenizer st = new StringTokenizer(line,",");
			while (st.hasMoreTokens())
			{
				//get next token and store it in the array
				//numbers[row][col] = st.nextToken();
				System.out.println(st.nextToken() + "\n");
				col++;
			}
			row++;
		}
		*/
		//norm.addOutputField(new OutputFieldRangeMapped(past_prices[0],0.1,0.9));
		for(int j = 0; j < input_size; j++)
			norm.addOutputField(new OutputFieldRangeMapped(past_prices[j],0.1,0.9));
		
		//norm.addOutputField(new OutputFieldRangeMapped(present_price,0.1,0.9));
		norm.process();
		
		return norm;
	}

	public void report(int current, int total, String message) {
		//System.out.println( current + "/" + total + " " + message );
	}
	
}
