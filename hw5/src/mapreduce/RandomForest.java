package mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.SortedMap;

import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ColumnFamilyRecordReader.Column;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import decisiontree.DTBuilder;

@SuppressWarnings("deprecation")
public class RandomForest extends Configured implements Tool {

	private static final Logger logger = LoggerFactory.getLogger(RandomForest.class);

	static int numOfTree = 10;// number of trees

	// Mapper to grow a tree separately
	public static class GrowTreeMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, Column>, Text, Text> {

		// The features of data
		private ArrayList<String> features;

		// initialize features
		public void setup(Context context) throws IOException, InterruptedException {
			features = new ArrayList<>();
			features.add("bidMean");
			features.add("askMean");
			features.add("diff");
			features.add("range");
			features.add("spread");
			features.add("lable");
		}

		public void map(ByteBuffer key, SortedMap<ByteBuffer, Column> columns, Context context)
				throws IOException, InterruptedException {
			// LongWritable index = new
			// LongWritable(context.getTaskAttemptID().getTaskID().getId());
			ArrayList<String> featureLeft = new ArrayList<>();
			ArrayList<ArrayList<Integer>> trainData = new ArrayList<ArrayList<Integer>>();
			String keyText = numOfTree + " trees";

			// random select sqrt(features)
			randomSelectFeatures(featureLeft);

			// read in training data
			for (Column column : columns.values()) {
				logger.debug("read " + key + ":" + column.name + " from " + context.getInputSplit());

				String record = ByteBufferUtil.string(column.name);
				String[] parts = record.split(",");
				ArrayList<Integer> recordInt = new ArrayList<>();
				for (int i = 0; i < parts.length; i++) {
					if (parts[i].equalsIgnoreCase("true"))
						recordInt.add(1);
					else
						recordInt.add(0);
				}
				trainData.add(recordInt);
			}

			// build decision tree
			DTBuilder dtBuilder = new DTBuilder(features);
			dtBuilder.train(trainData, featureLeft);

			Gson gson = new Gson();
			context.write(new Text(keyText), new Text(gson.toJson(dtBuilder)));

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
	}

	// Reducer to aggregate trees to a forest, only 1 reducer
	public static class AggregateTreeReducer extends Reducer<Text, Text, Text, Text> {

		private class Forest {
			private ArrayList<DTBuilder> trees;

			public Forest() {
				trees = new ArrayList<>();
			}

			public ArrayList<DTBuilder> getForest() {
				return trees;
			}
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Forest forest = new Forest();
			Gson gson = new Gson();

			for (Text tree : values) {
				forest.trees.add(gson.fromJson(tree.toString(), DTBuilder.class));
			}

			context.write(key, new Text(gson.toJson(forest)));
		}

	}

	// configuration
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "RandomForest");
		job.setJarByClass(RandomForest.class);
		job.setMapperClass(GrowTreeMapper.class);
		job.setCombinerClass(AggregateTreeReducer.class);
		job.setReducerClass(AggregateTreeReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path("output"));

		job.setInputFormatClass(ColumnFamilyInputFormat.class);

		ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
		ConfigHelper.setInputInitialAddress(job.getConfiguration(), "10.0.0.4");
		ConfigHelper.setInputPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
		ConfigHelper.setInputColumnFamily(job.getConfiguration(), "bigdata", "train");
		ConfigHelper.setRangeBatchSize(conf, 8000 / numOfTree); // Total 10
																// trees

		SlicePredicate predicate = new SlicePredicate()
				.setSlice_range(new SliceRange().setStart(ByteBufferUtil.EMPTY_BYTE_BUFFER)
						.setFinish(ByteBufferUtil.EMPTY_BYTE_BUFFER).setCount(10000));
		ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new RandomForest(), args);
		System.exit(0);
	}
}
