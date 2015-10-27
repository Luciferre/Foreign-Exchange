package randomforest;

public class Test {
	// This test deserializes the forest and tests the remaining 20% test
	// records
	public static void main(String args[]) {
		RFBuilder rfBuilder = new RFBuilder(1000);
		// rfBuilder.readFile("test.csv");
		rfBuilder.readFromDatabase();
		rfBuilder.deserializeForest();
		System.out.println("Accuracy of 100 trees: " + (1 - rfBuilder.test(rfBuilder.getAllData())));

	}
}
