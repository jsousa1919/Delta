package delta.market;

import java.io.IOException;
import java.io.File;
import java.util.Scanner;

public class Main {

	public static void main(String args[]) throws IOException{
		//First, set up Config
		Scanner input = new Scanner( System.in );
		String answer;
		System.out.print("Data directory: ");
		answer = input.next();
		Config.START_DIR = answer + "/";
		
		String files;
		File currFolder;
		String currFolderName;
		File folder = new File(Config.START_DIR);
		File[] folderList = folder.listFiles();
		int k;
		for(k = 0; k < folderList.length; k++){
			currFolder = folderList[k];
			currFolderName = currFolder.getName();
			if (currFolder.isDirectory()){
				//System.out.println(currFolder.getName());
				Config.DATA_DIR = Config.START_DIR + currFolderName + "/";
				Config.TRAINING_FILENAME = currFolderName + ".train.csv";
				Config.TEST_FILENAME = currFolderName + ".test.csv";
				System.out.println(currFolderName);

				//Config.DATA_DIR += answer + '/';
				//Config.BASE_DIR += answer + '/';
				
				/*
				System.out.print("Subdirectory: ");
				answer = input.next();
				Config.BASE_DIR += answer + '/';
				*/
					/*
				System.out.print("Training file: ");
				answer = input.next();
				Config.TRAINING_FILENAME = answer;
				
				System.out.print("Testing file: ");
				answer = input.next();
				Config.TEST_FILENAME = answer;
				*/
				String[] networks = {"feedforward", "elman", "jordan"};
				
				for( int i = 0; i < networks.length; i++){
					//setup subdirectory
					System.out.println(networks[i]);
					Config.BASE_DIR = Config.DATA_DIR + networks[i] + '/';
					Config.NETWORK_FILENAME = networks[i] + ".eg";
					Config.EVALUATE_OUTPUT = networks[i] + "-out.csv";
					Config.setupConfig();
					Config.writeInfo(networks[i] + "-config.txt");
					
					if (networks[i].equals("feedforward")){
		
						Config.NETWORK_TYPE = 0;
						
					}
					else if (networks[i].equals("elman")){
						Config.NETWORK_TYPE = 1;
					}
					else {
						Config.NETWORK_TYPE = 2;
						
					}
					/*
					System.out.print("Network file: ");
					answer = input.next();
					Config.NETWORK_FILENAME = answer;
					
					System.out.print("Output name: ");
					answer = input.next();
					Config.EVALUATE_OUTPUT = answer;		
					*/
					//Config.setupConfig();
					/*
					System.out.print("Config filename: ");
					answer = input.next();
					Config.writeInfo(answer);
					
					boolean norm = false, train = false, evaluate = false; 
					System.out.print("Generate norm: ");
					answer = input.next();
					if(answer.equals("yes")){
						norm = true;
					}
					
					System.out.print("Train network: ");
					answer = input.next();
					if(answer.equals("yes")){
						train = true;
						System.out.print("Network type (0 - feed, 1 - elman, 2 - jordan: ");
						answer = input.next();
						Config.NETWORK_TYPE = Integer.parseInt(answer);
					}
					
					System.out.print("Evaluate network: ");
					answer = input.next();
					if(answer.equals("yes")){
						evaluate = true;
					}
					*/
					boolean norm = true;
					boolean train = true;
					boolean evaluate = true;
					//now normalize data
					if (norm){
						GenerateData data_gen = new GenerateData();
						data_gen.createNormData(true);
						data_gen.createNormData(false);
					}
					
					if (train){
						Train train_net = new Train();
						train_net.train(networks[i]);
					}
					
					if (evaluate){
					Evaluate eval = new Evaluate();
					eval.evaluate();
					}
					
					System.out.println("Finished");
				
				}
			}
		}
		System.out.println("Finished all");
	}
}
