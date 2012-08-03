package delta.market;

import java.util.Iterator;

import org.encog.neural.data.NeuralDataPair;
import org.encog.neural.data.NeuralDataSet;
import org.encog.neural.data.buffer.BufferedNeuralDataSet;
import org.encog.neural.networks.BasicNetwork;
import org.encog.neural.networks.training.propagation.back.Backpropagation;
import org.encog.neural.networks.training.propagation.resilient.ResilientPropagation;
import org.encog.neural.networks.training.strategy.RequiredImprovementStrategy;
import org.encog.normalize.DataNormalization;
import org.encog.persist.EncogPersistedCollection;
import org.encog.util.logging.Logging;
import org.encog.util.simple.EncogUtility;
import org.encog.neural.networks.training.propagation.resilient.ResilientPropagation;

public class Train {

	public void train(String network_type){
		//System.out.println("Converting training file to binary");
		EncogPersistedCollection encog = new EncogPersistedCollection(
				Config.NETWORK_FILE);
		DataNormalization norm = (DataNormalization) encog
				.find(Config.NORMALIZATION_NAME);

		//System.out.println(norm.getNetworkInputLayerSize() + " " + norm.getNetworkOutputLayerSize());

		//EncogUtility.convertCSV2Binary(Config.NORMALIZED_FILE, Config.BINARY_FILE, norm.getNetworkInputLayerSize() - 1, 1, false);
		EncogUtility.convertCSV2Binary(Config.NORMALIZED_FILE, Config.BINARY_FILE, Config.INPUT_COUNT - 1, 1, false);
		BufferedNeuralDataSet trainingSet = new BufferedNeuralDataSet(Config.BINARY_FILE);
		
		GenerateNetwork genNet = new GenerateNetwork();
		BasicNetwork network;
		switch (Config.NETWORK_TYPE){
			case 0:
				network = genNet.generateFeedforward(trainingSet);
				break;
			case 1:
				network = genNet.generateElman(trainingSet);
				break;
			case 2:
				network = genNet.generateJordan(trainingSet);
				break;
			default:
				return;
				
		}
		Logging.stopConsoleLogging();

		
		final ResilientPropagation train = new ResilientPropagation(network, trainingSet);
		//train.addStrategy(new RequiredImprovementStrategy(5));
		train.addStrategy(new RequiredImprovementStrategy(5));
		int epoch = 1;
		final long startTime = System.currentTimeMillis();
		int left = 0;

		do {
			
			final int running =	(int) ((System.currentTimeMillis() - startTime) / 60000);
			left = Config.TRAINING_MINUTES - running;

			train.iteration();
			
			NeuralDataSet train_data = train.getTraining();
			//System.out.println(train.getNetwork().dumpWeights());
			/*Iterator itr = train_data.iterator();
			while(itr.hasNext()){
				
				NeuralDataPair ndp = (NeuralDataPair) itr.next();
				System.out.println(ndp.getInput() + " ideal is" + ndp.getIdeal());
				
			}*/
			//System.out.println("Epoch #" + epoch + " Error:" + train.getError() + " Time Left: " + left + " Minutes");
			epoch++;
		} while((train.getError() > 0.0005) && (left >= 0));
	
		trainingSet.close();
		
		Config.appendConfig("Network: " + network_type + " Final Error: " + train.getError() + " Iterations: " + epoch);
		
		//System.out.println("Training complete, saving network...");
		encog.add(Config.TRAINED_NETWORK_NAME, network);
		
	}
	
}
