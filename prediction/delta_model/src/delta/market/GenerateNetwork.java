package delta.market;

import org.encog.engine.network.activation.ActivationSigmoid;
import org.encog.neural.data.NeuralDataSet;
import org.encog.neural.data.buffer.BufferedNeuralDataSet;
import org.encog.neural.networks.BasicNetwork;
import org.encog.neural.networks.layers.BasicLayer;
import org.encog.neural.networks.layers.ContextLayer;
import org.encog.neural.networks.layers.Layer;
import org.encog.neural.networks.logic.FeedforwardLogic;
import org.encog.neural.networks.synapse.SynapseType;
import org.encog.normalize.DataNormalization;
import org.encog.persist.EncogPersistedCollection;
import org.encog.util.simple.EncogUtility;
/*
 * 
 * Wrapper class for the network itself
 * 
 */

public class GenerateNetwork {

	public BasicNetwork generateFeedforward(NeuralDataSet trainingSet){
		BasicNetwork network = new BasicNetwork();
			
		network.addLayer(new BasicLayer(new ActivationSigmoid(), true,
				trainingSet.getInputSize()));
		network.addLayer(new BasicLayer(new ActivationSigmoid(), true,
				Config.HIDDEN_COUNT));
		network.addLayer(new BasicLayer(new ActivationSigmoid(), true,
				trainingSet.getIdealSize()));
		//System.out.println("input size: " + trainingSet.getInputSize() + " ideal size: " + trainingSet.getIdealSize());
		network.setLogic(new FeedforwardLogic());
		network.getStructure().finalizeStructure();
		network.reset();
		return network;
		
	}	
	
	public BasicNetwork generateElman(NeuralDataSet trainingSet){
		Layer context = new ContextLayer(Config.HIDDEN_COUNT);
		BasicNetwork network = new BasicNetwork();
		network.addLayer(new BasicLayer(new ActivationSigmoid(), true,
				trainingSet.getInputSize()));
		Layer hidden = new BasicLayer(new ActivationSigmoid(), true,
				Config.HIDDEN_COUNT);
		network.addLayer(hidden);
		hidden.addNext(context, SynapseType.OneToOne);
		context.addNext(hidden);
		network.addLayer(new BasicLayer(new ActivationSigmoid(), true,
				trainingSet.getIdealSize()));
		//System.out.println("input size: " + trainingSet.getInputSize() + " ideal size: " + trainingSet.getIdealSize());
		//network.setLogic(new FeedforwardLogic());
		network.getStructure().finalizeStructure();
		network.reset();
		return network;
		
	}	
	
	public BasicNetwork generateJordan(NeuralDataSet trainingSet){
		Layer context = new ContextLayer(trainingSet.getIdealSize());
		BasicNetwork network = new BasicNetwork();
		network.addLayer(new BasicLayer(new ActivationSigmoid(), true,
				trainingSet.getInputSize()));
		Layer hidden = new BasicLayer(new ActivationSigmoid(), true,
				Config.HIDDEN_COUNT);
		network.addLayer(hidden);
		Layer output = new BasicLayer(new ActivationSigmoid(), true,
				trainingSet.getIdealSize());
		//System.out.println("input size: " + trainingSet.getInputSize() + " ideal size: " + trainingSet.getIdealSize());
		network.addLayer(output);
		//network.setLogic(new FeedforwardLogic());
		output.addNext(context, SynapseType.OneToOne);
		context.addNext(hidden);
		network.getStructure().finalizeStructure();
		network.reset();
		return network;
		
	}
	
}
