package cassandra;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraConnector {
	/** Cassandra Cluster. */
	private Cluster cluster;
	/** Cassandra Session. */
	private Session session;

	/**
	 * Connect to Cassandra Cluster specified by provided node IP address and
	 * port number.
	 *
	 * @param node
	 *            Cluster node IP address.
	 * @param port
	 *            Port of cluster host.
	 */
	public void connect() {
		// Connect to the cluster and keyspace "demo"
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect("bigdata");

	}

	// read data from csv and save to cassandra
	public void insertData() {

		try {
			FileReader fileReader = new FileReader("test.csv");
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line = bufferedReader.readLine();

			// initialize all data records
			while ((line = bufferedReader.readLine()) != null) {
				String[] parts = line.split(",");
//				if (parts[0].equals("0"))
//					bidMean = "false";
//				else
//					bidMean = "true";
//				if (parts[1].equals("0"))
//					askMean = "false";
//				else
//					askMean = "true";
//				if (parts[2].equals("0"))
//					diff = "false";
//				else
//					diff = "true";
//				if (parts[3].equals("0"))
//					range = "false";
//				else
//					range = "true";
//				if (parts[4].equals("0"))
//					spread = "false";
//				else
//					spread = "true";
//				if (parts[5].equals("0"))
//					lable = "false";
//				else
//					lable = "true";

				String command = String.format(
						"INSERT INTO test (rid, bidMean, askMean, diff, range, spread, lable) VALUES (now(), %s, %s, %s, %s, %s, %s)",
						parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]);
				session.execute(command);

			}
			bufferedReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Provide my Session.
	 *
	 * @return My session.
	 */
	public Session getSession() {
		return this.session;
	}

	/** Close cluster. */
	public void close() {
		cluster.close();
	}

	/**
	 * Main function for demonstrating connecting to Cassandra with host and
	 * port.
	 *
	 * @param args
	 *            Command-line arguments; first argument, if provided, is the
	 *            host and second argument, if provided, is the port.
	 */
	public static void main(final String[] args) {
		final CassandraConnector client = new CassandraConnector();
		client.connect();
		client.insertData();
		client.close();
	}
}
