package randomforest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.io.Files;
import com.google.gson.Gson;

import cassandra.CassandraConnector;
import decisiontree.DTBuilder;

//This is a builder of random forest
public class RFBuilder {

	// The features of data
	private ArrayList<String> features;

	// all the data records
	private ArrayList<ArrayList<Integer>> allData = new ArrayList<>();

	// the number of trees in forest
	private int numOfTrees;

	// the list of forest
	// private ArrayList<DTBuilder> forest;
	private class Forest {
		private ArrayList<DTBuilder> trees;

		public ArrayList<DTBuilder> getForest() {
			return trees;
		}
	}

	Forest forest = new Forest();

	// the list of errorRate
	private ArrayList<Float> accuracies;

	// cassanddra connector
	CassandraConnector connector = new CassandraConnector();

	public RFBuilder(int numOfTrees) {
		this.numOfTrees = numOfTrees;
		this.features = new ArrayList<>();
		this.allData = new ArrayList<>();
		this.accuracies = new ArrayList<>();
		this.forest.trees = new ArrayList<>();
	}

	// read records from file
	public void readFile(String fileName) {

		try {
			FileReader fileReader = new FileReader(fileName);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line = bufferedReader.readLine();
			String parts[] = line.split(",");
			features.addAll(Arrays.asList(parts));// Add features

			// initialize all data records
			while ((line = bufferedReader.readLine()) != null) {
				parts = line.split(",");
				ArrayList<Integer> arrayList = new ArrayList<>();
				for (int i = 0; i < parts.length; i++) {
					int tmp = Integer.parseInt(parts[i]);
					arrayList.add(tmp);
				}
				allData.add(arrayList);
			}
			bufferedReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// read records from database
	public void readFromDatabase() {
		// initialize feature
		features.add("bidMean");
		features.add("askMean");
		features.add("diff");
		features.add("range");
		features.add("spread");
		features.add("lable");

		connector.connect();
		String command = "select * from train";
		ResultSet results = connector.getSession().execute(command);
		for (Row row : results) {
			ArrayList<Integer> arrayList = new ArrayList<>();
			for (int i = 1; i < 7; i++) {
				if (row.getBool(i))
					arrayList.add(1);
				else
					arrayList.add(0);
			}
			allData.add(arrayList);
		}
	}

	// train the forest
	public void train() {
		for (int i = 0; i < this.numOfTrees; i++) {
			DTBuilder dtBuilder = new DTBuilder(features);
			ArrayList<ArrayList<Integer>> trainData = new ArrayList<>();
			ArrayList<ArrayList<Integer>> testData = new ArrayList<>();
			ArrayList<String> featureLeft = new ArrayList<>();

			// random select sqrt(features) and 2/3 train data
			randomSelectFeatures(featureLeft);
			randomSelectRecords(trainData, testData);

			// train a decision tree
			dtBuilder.train(trainData, featureLeft);
			forest.trees.add(dtBuilder);

			// calculate error rate of the forest and persist data
			float errRate = test(testData);
			accuracies.add(1 - errRate);
		}
	}

	// random select sqrt(#features)
	public void randomSelectFeatures(ArrayList<String> featureLeft) {
		int n = features.size() - 1;
		int sqrt = (int) Math.sqrt(n);
		while (true) {
			String tmpFeatrue = features.get((int) (Math.random() * n));
			if (!featureLeft.contains(tmpFeatrue)) {
				featureLeft.add(tmpFeatrue);
				sqrt--;
			}
			if (sqrt == 0)
				break;
		}

	}

	// random select 2/3 #records
	public void randomSelectRecords(ArrayList<ArrayList<Integer>> trainData, ArrayList<ArrayList<Integer>> testData) {
		int oneThird = (int) (0.667 * allData.size());
		int allCount = allData.size();
		int twoThird = allCount - oneThird;

		ArrayList<Integer> trainDataId = new ArrayList<>();

		for (int i = 0; i < oneThird; i++) {
			int id = (int) (Math.random() * allCount);
			trainData.add(allData.get(id));
			trainDataId.add(id);
		}

		for (int i = 0; i < allCount; i++) {
			if (!trainDataId.contains(i)) {
				testData.add(allData.get(i));
			}
			if (testData.size() == twoThird)
				break;
		}
	}

	// test left 1/3 records
	public float test(ArrayList<ArrayList<Integer>> testData) {
		int errorCount = 0;

		for (int i = 0; i < testData.size(); i++) {
			ArrayList<Integer> arrayList = testData.get(i);
			int result = getDecision(arrayList);
			if (result != arrayList.get(arrayList.size() - 1))
				errorCount++;// if the prediction is different with the
								// label, error++
		}

		return (float) errorCount / testData.size();

	}

	// get vote decision from forest
	public int getDecision(ArrayList<Integer> arrayList) {
		int numOfPosTree = 0;
		int numOfNegTree = 0;

		for (int i = 0; i < forest.trees.size(); i++) {
			int result = forest.trees.get(i).getDecision(arrayList);
			if (result == 1) {
				numOfPosTree++;
			} else {
				numOfNegTree++;
			}
		}
		if (numOfPosTree >= numOfNegTree)
			return 1;
		return 0;
	}

	// print error rate list
	public void printAccuracyList() {
		for (int i = 0; i < accuracies.size(); i++) {
			System.out.println("Accuracy of " + (i + 1) + " trees: " + accuracies.get(i));
		}
	}

	// save performance metrics to cassandra
	public void saveAccuracyList() {
		for (int i = 0; i < accuracies.size(); i++) {
			String command = String.format("INSERT INTO performance (number, accuracy) VALUES (%d, %f)", i + 1,
					accuracies.get(i));
			connector.getSession().execute(command);
		}
		connector.close();

	}

	// serialize automotive object and save to file
	public void serializeForest() {
		try {
			// ObjectOutputStream out = new ObjectOutputStream(new
			// FileOutputStream("RandomForest"));
			PrintWriter out = new PrintWriter("RandomForest.json");
			Gson gson = new Gson();
			String serializedForest = gson.toJson(forest);
			out.print(serializedForest);
			// out.writeObject(serializedForest);
			out.close();
		} catch (Exception e) {
			System.out.print("Error:" + e);
			System.exit(1);
		}

	}

	// read file and deserialize automotive object
	@SuppressWarnings("unchecked")
	public void deserializeForest() {
		ObjectInputStream in;
		try {
			String json = FileUtils.readFileToString(new File("RandomForest.json"));
			// in = new ObjectInputStream(new FileInputStream("RandomForest"));
			Gson gson = new Gson();
			forest = gson.fromJson(json, Forest.class);
			// forest = (ArrayList<DTBuilder>) in.readObject();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public ArrayList<ArrayList<Integer>> getAllData() {
		return allData;
	}

	public static void main(String args[]) {
		RFBuilder rfBuilder = new RFBuilder(100);
		// rfBuilder.readFile("train.csv");
		rfBuilder.readFromDatabase();
		rfBuilder.train();
		// rfBuilder.printAccuracyList();
		rfBuilder.saveAccuracyList();
		rfBuilder.serializeForest();
	}
}
