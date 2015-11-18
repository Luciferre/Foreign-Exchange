package decisiontree;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class DTBuilder implements Serializable {

	private Node root;

	// list name of features
	private ArrayList<String> features = new ArrayList<>();

	// All data set
	private ArrayList<ArrayList<Integer>> allData = new ArrayList<>();

	// featrues still needed to be split
	private ArrayList<String> featureLeft = new ArrayList<>();

	// Decision Tree node class
	public class Node implements Serializable {
		String name;
		int numOfPos = 0;
		int numOfNeg = 0;
		Node leftNode;
		Node rightNode;

		public Node(String name) {
			this.name = name;
		}

		// Calculate the entropy of the node
		public double calculateEntropy() {
			if (numOfPos == 0 || numOfNeg == 0)
				return 0;

			double entropy = (double) numOfPos / (numOfPos + numOfNeg)
					* (Math.log((double) numOfPos / (numOfPos + numOfNeg)) / Math.log(2))
					+ (double) numOfNeg / (numOfPos + numOfNeg)
							* (Math.log((double) numOfNeg / (numOfPos + numOfNeg)) / Math.log(2));
			return entropy;

		}

		// Calculate the information gain of the node
		public double calculateInfoGain() {
			double infoGain = calculateEntropy()
					- (leftNode.numOfNeg + leftNode.numOfPos) / (numOfNeg + numOfPos) * leftNode.calculateEntropy()
					- (rightNode.numOfNeg + rightNode.numOfPos) / (numOfNeg + numOfPos) * rightNode.calculateEntropy();

			return -infoGain;
		}
	}

	public DTBuilder() {
		root = new Node(null);
	}

	public DTBuilder(ArrayList<String> features) {
		root = new Node(null);
		this.features = features;
	}

	// Read data file and initialize the root of Decision Tree
	public void readFile(String fileName) {

		try {
			FileReader fileReader = new FileReader(fileName);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line = bufferedReader.readLine();
			String parts[] = line.split(",");
			features.addAll(Arrays.asList(parts));// Add features
			featureLeft.addAll(features);
			featureLeft.remove(features.size() - 1);

			while ((line = bufferedReader.readLine()) != null) {
				parts = line.split(",");
				ArrayList<Integer> arrayList = new ArrayList<>();
				int tmp = 0;
				for (int i = 0; i < parts.length; i++) {
					tmp = Integer.parseInt(parts[i]);
					arrayList.add(tmp);
				}
				if (tmp == 0) // initialize root node
					root.numOfNeg++;
				else
					root.numOfPos++;
				allData.add(arrayList);
			}
			bufferedReader.close();
		} catch (IOException e) {
			System.out.println("Error ­­ " + e.toString());
		}

		trainTree(root, allData, featureLeft);

	}

	// Recursive training decision tree
	public void train(ArrayList<ArrayList<Integer>> data, ArrayList<String> featureLeft) {

		for (int i = 0; i < data.size(); i++) {
			ArrayList<Integer> tmpData = data.get(i);
			// initialize root node
			if (tmpData.get(tmpData.size() - 1) == 1) {
				root.numOfPos++;
			} else {
				root.numOfNeg++;
			}
		}

		trainTree(root, data, featureLeft);
	}

	// Recursive training decision tree
	public void trainTree(Node node, ArrayList<ArrayList<Integer>> data, ArrayList<String> featureLeft) {

		// End of the recursion
		if (node.numOfNeg == 0 || node.numOfPos == 0 || featureLeft.isEmpty()) {
			return;
		}

		// Calculate the node with max information gain among the left features
		double maxInfoGain = 0;
		int maxFeatureID = 0;
		Node maxNode = null;

		for (int j = 0; j < featureLeft.size(); j++) {
			String tmpFeature = featureLeft.get(j);
			int tmpFeatureID = 0;
			for (int i = 0; i < features.size(); i++) {
				if (features.get(i).equals(tmpFeature))
					tmpFeatureID = i;
			}

			Node tmp = calculateNode(node, data, featureLeft.get(j), tmpFeatureID);
			double tmpInfoGain = tmp.calculateInfoGain();
			if (tmpInfoGain > maxInfoGain) {
				maxInfoGain = tmpInfoGain;
				maxFeatureID = j;
				maxNode = tmp;
			}
		}

		if (maxInfoGain <= 0) {
			node.name = null;
			node.leftNode = null;
			node.rightNode = null;
			return;
		}

		// split the data by the max info's feature
		node.leftNode = maxNode.leftNode;
		node.rightNode = maxNode.rightNode;
		node.name = maxNode.name;
		node.numOfNeg = maxNode.numOfNeg;
		node.numOfPos = maxNode.numOfPos;

		String maxFeature = featureLeft.get(maxFeatureID);
		int finalFeatureID = 0;
		for (int i = 0; i < features.size(); i++) {
			if (features.get(i).equals(maxFeature))
				finalFeatureID = i;
		}

		ArrayList<ArrayList<Integer>> leftData = new ArrayList<>();
		ArrayList<ArrayList<Integer>> rightData = new ArrayList<>();

		for (int i = 0; i < data.size(); i++) {
			ArrayList<Integer> tmpData = data.get(i);
			if (tmpData.get(finalFeatureID) == 1) {
				leftData.add(tmpData);
			} else {
				rightData.add(tmpData);

			}
		}

		featureLeft.remove(maxFeatureID);
		// train sub trees
		trainTree(node.leftNode, leftData, new ArrayList<>(featureLeft));
		trainTree(node.rightNode, rightData, new ArrayList<>(featureLeft));
	}

	// create a new feature node
	public Node calculateNode(Node node, ArrayList<ArrayList<Integer>> data, String name, int featureID) {
		Node tmp = new Node(name);
		tmp.numOfNeg = node.numOfNeg;
		tmp.numOfPos = node.numOfPos;
		tmp.leftNode = new Node(null);
		tmp.rightNode = new Node(null);

		for (int i = 0; i < data.size(); i++) {
			ArrayList<Integer> tmpData = data.get(i);
			if (tmpData.get(featureID) == 1) { // positive features
				if (tmpData.get(tmpData.size() - 1) == 1) {
					tmp.leftNode.numOfPos++;
				} else {
					tmp.leftNode.numOfNeg++;
				}
			} else {// negative features
				if (tmpData.get(tmpData.size() - 1) == 1) {
					tmp.rightNode.numOfPos++;
				} else {
					tmp.rightNode.numOfNeg++;
				}
			}
		}
		return tmp;
	}

	// test the decision tree and calculate the error rate
	public void testTree(String fileName) {
		try {
			FileReader fileReader = new FileReader(fileName);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;
			int count = 0, errorCount = 0;
			bufferedReader.readLine();
			while ((line = bufferedReader.readLine()) != null) {
				String[] parts = line.split(",");
				ArrayList<Integer> arrayList = new ArrayList<>();
				for (int i = 0; i < parts.length; i++) {
					int tmp = Integer.parseInt(parts[i]);
					arrayList.add(tmp);

				}
				int result = getDecision(arrayList, root);
				if (result != arrayList.get(arrayList.size() - 1))
					errorCount++;
				count++;
			}
			bufferedReader.close();
			System.out.println("Accuracy: " + (1 - (double) errorCount / count));
		} catch (IOException e) {
			System.out.println("Error ­­ " + e.toString());
		}

	}

	// get the prediction of a record in decision tree
	public int getDecision(ArrayList<Integer> arrayList, Node node) {
		if (node.name == null) {
			if (node.numOfPos > node.numOfNeg)
				return 1;
			else
				return 0;
		}

		for (int i = 0; i < features.size(); i++) {
			if (node.name.equals(features.get(i))) {
				if (arrayList.get(i) == 1) {
					return getDecision(arrayList, node.leftNode);
				} else {
					return getDecision(arrayList, node.rightNode);
				}
			}
		}

		return 0;
	}

	// get the prediction of a record in decision tree
	public int getDecision(ArrayList<Integer> arrayList) {
		return getDecision(arrayList, root);
	}

	public static void main(String[] args) {
		DTBuilder dtBuilder = new DTBuilder();
		dtBuilder.readFile("train.csv");
		dtBuilder.testTree("test.csv");

	}
}
