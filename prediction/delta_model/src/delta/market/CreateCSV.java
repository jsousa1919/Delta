package delta.market;

import org.encog.neural.data.NeuralDataPair;
import org.encog.neural.data.market.MarketDataDescription;
import org.encog.neural.data.market.MarketDataType;
import org.encog.neural.data.market.MarketNeuralDataSet;
import org.encog.neural.data.market.loader.MarketLoader;
import org.encog.neural.data.market.loader.YahooFinanceLoader;
import java.io.FileWriter;
import java.io.IOException;
import com.Ostermiller.util.CSVPrinter;

/*
 * 
 * For creating a temp csv with price data
 * 
 */

public class CreateCSV {
	public void makeCSV(boolean train_set) throws IOException{
		
		final MarketLoader loader = new YahooFinanceLoader();
		/*final MarketNeuralDataSet market = new MarketNeuralDataSet(loader,
				Config.INPUT_WINDOW, Config.PREDICT_WINDOW);
		final MarketDataDescription desc = new MarketDataDescription(
				Config.TICKER, MarketDataType.ADJUSTED_CLOSE, true, true);
		market.addDescription(desc);
		if(train_set)
			market.load(Config.TRAIN_BEGIN.getTime(), Config.TRAIN_END.getTime());
		else
			market.load(Config.TEST_BEGIN.getTime(), Config.TEST_END.getTime());
			
		market.generate();
		market.setDescription("Market data for: " + Config.TICKER.getSymbol());
		java.util.List<NeuralDataPair> mp = market.getData();
		String outputcsv = "";
		//System.out.println("hello");
		if(train_set)
			outputcsv = Config.BASE_DIR + Config.TRAINING_NAME;
		else
			outputcsv = Config.BASE_DIR + Config.TEST_NAME;
		FileWriter fstream = new FileWriter(outputcsv);
		//item [BasicNeuralData:-0.02285245517403027,-0.012592947949148444,-0.05016397424996958,0.05920716112531963,0.06676325003018231,0.0,0.014486192847442295,0.012159750111557378,0.038245343326352904,-0.012738853503184744]
		CSVPrinter csvp = new CSVPrinter(fstream, '~');
		int j = 0;
		for (int i=0; i< mp.size(); i++)
		  {
				String[] csv_data = new String[Config.INPUT_WINDOW + Config.PREDICT_WINDOW];
				double[] data_dbls = mp.get(i).getInput().getData();
				for(j=0; j < data_dbls.length; j++){
					//System.out.println("item " + data_dbls[j]);
					csv_data[j] = data_dbls[j] + "";
					//System.out.println("j is " + j);
				}
				//System.out.println("j is " + j + " length si " + data_dbls.length);
				csv_data[j] = mp.get(i).getIdeal().getData()[0] + "";
				//System.out.println(mp.get(i));
			  //System.out.println( lList.get(i) );
				csvp.writeln(csv_data);
		  }
		//csvp.writeln(temp);*/
	}
}
