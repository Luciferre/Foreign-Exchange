package preparedata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.NetworkInterface;
import java.util.ArrayList;

import javax.xml.crypto.Data;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class FeatureBuilder {
	// diff = the last record bid - the first record bid
	// range = the max bid - min bid
	// spread = ask - bid
	// array: 0 time, 1 bid, 2 ask,
	static ArrayList<ArrayList<String>> data = new ArrayList<>();
	// 0 bidMean, 1 askMean, 2 diff, 3 range, 4 spread, 5 lable
	static ArrayList<ArrayList<Float>> processedData = new ArrayList<>();
	static ArrayList<ArrayList<Integer>> binaryData = new ArrayList<>();
	float avgBidMean = 0, avgAskMean = 0, diff = 0, avgRange = 0, avgSpread = 0;

	public void readFile(String filename) {
		try {
			FileReader fileReader = new FileReader(filename);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				String parts[] = line.split(",");
				ArrayList<String> arrayList = new ArrayList<>();

				arrayList.add(parts[1].trim());
				arrayList.add(parts[2].trim());
				arrayList.add(parts[3].trim());
				data.add(arrayList);
			}
			bufferedReader.close();
		} catch (IOException e) {
			System.out.println("Error ­­ " + e.toString());
		}
	}

	public void compute(int index, int minutes) {
		float max = Float.MIN_VALUE;
		float min = Float.MAX_VALUE;
		float bidMean = 0, spread = 0, askMean = 0, range = 0, diff = 0;
		float last = 0;
		int lable = 1;
		ArrayList<String> base = data.get(index);
		ArrayList<Float> newBase = new ArrayList<>();
		DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMMdd HH:mm:ss.SSS");
		DateTime baseDateTime = formatter.parseDateTime(base.get(0));
		DateTime previousDateTime = baseDateTime.minusMinutes(minutes);

		float bid = Float.parseFloat(base.get(1));
		float ask = Float.parseFloat(base.get(2));
		spread = ask - bid;

		int count = 0;
		for (int i = index; i >= 0; i--) {
			ArrayList<String> tmpList = data.get(i);
			DateTime tmpDateTime = formatter.parseDateTime(tmpList.get(0));
			if (tmpDateTime.isBefore(previousDateTime))
				break;

			float tmpbid = Float.parseFloat(tmpList.get(1));
			float tmpask = Float.parseFloat(tmpList.get(2));
			if (i == index)
				last = tmpbid;
				diff = last - tmpbid;
			if (max < tmpbid)
				max = tmpbid;
			if (min > tmpbid)
				min = tmpbid;
			bidMean += tmpbid;
			askMean += tmpask;
			count++;
		}

		for (int i = index; i < data.size(); i++) {
			float newbid = Float.parseFloat(data.get(i).get(1));
			if (newbid > bid) {
				lable = 1;
				break;
			} else if (newbid < bid) {
				lable = 0;
				break;
			}
		}

		// newBase.add((float) baseDateTime.getMinuteOfDay());
		newBase.add(bidMean / count);
		newBase.add(askMean / count);
		newBase.add(diff);
		newBase.add(max - min);
		newBase.add(spread);
		newBase.add((float) lable);// The lable should be the last one

		processedData.add(newBase);
	}

	public void computeThreshhold() {
		int size = processedData.size();
		for (int i = 0; i < size; i++) {
			avgBidMean += processedData.get(i).get(0);
			avgAskMean += processedData.get(i).get(1);
			avgRange += processedData.get(i).get(3);
			avgSpread += processedData.get(i).get(4);
		}

		avgBidMean /= size;
		avgAskMean /= size;
		avgRange /= size;
		avgSpread /= size;

		System.out.println("Treshhold: avgBidMean " + avgBidMean + ", avgAskMean " + avgAskMean + ", diff " + diff
				+ ", avgRange " + avgRange + ", avgSpread " + avgSpread);
	}

	public void buildBinaryLable() {
		int size = processedData.size();
		for (int i = 0; i < size; i++) {
			ArrayList<Integer> tmpBinary = new ArrayList<>();
			ArrayList<Float> tmpBase = processedData.get(i);
			if (tmpBase.get(0) >= avgBidMean)
				tmpBinary.add(1);
			else
				tmpBinary.add(0);

			if (tmpBase.get(1) >= avgAskMean)
				tmpBinary.add(1);
			else
				tmpBinary.add(0);

			if (tmpBase.get(2) >= 0)
				tmpBinary.add(1);
			else
				tmpBinary.add(0);

			if (tmpBase.get(3) >= avgRange)
				tmpBinary.add(1);
			else
				tmpBinary.add(0);

			if (tmpBase.get(4) >= avgSpread)
				tmpBinary.add(1);
			else
				tmpBinary.add(0);

			tmpBinary.add(tmpBase.get(5).intValue());
			binaryData.add(tmpBinary);
		}
	}

	public void writeTrainFile(String fileName) {
		int fileLine = (int) (binaryData.size() * 0.8);
		File file = new File(fileName);
		try {
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write("bidMean,askMean,diff,range,spread,lable\n");

			for (int i = 0; i < fileLine; i++) {// skip first 100
												// records
				ArrayList<Integer> tmpArrayList = binaryData.get(i);
				// ArrayList<Integer> nextArrayList = binaryData.get(i + 1);
				for (int j = 0; j < tmpArrayList.size(); j++) {
					if (j == 0)
						bw.write("" + tmpArrayList.get(j));
					// else if (j == tmpArrayList.size() - 1)
					// bw.write("," + nextArrayList.get(j));
					else
						bw.write("," + tmpArrayList.get(j));

				}

				bw.write("\n");

			}
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void writeTestFile(String fileName) {
		int index = (int) (binaryData.size() * 0.8);

		File file = new File(fileName);
		try {
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write("bidMean,askMean,diff,range,spread,lable\n");
			for (int i = index; i < binaryData.size(); i++) {// skip first 100
																// records
				ArrayList<Integer> tmpArrayList = binaryData.get(i);
				// ArrayList<Integer> nextArrayList = binaryData.get(i + 1);
				for (int j = 0; j < tmpArrayList.size(); j++) {
					if (j == 0)
						bw.write("" + tmpArrayList.get(j));
					// else if (j == tmpArrayList.size() - 1)
					// bw.write("," + nextArrayList.get(j));
					else
						bw.write("," + tmpArrayList.get(j));

				}

				bw.write("\n");

			}
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String args[]) {
		FeatureBuilder buildFeature = new FeatureBuilder();
		buildFeature.readFile("sample.csv");
		int count = 0;
		for (int i = 0; i < data.size(); i++) {
			buildFeature.compute(i, 5);
			if (count % 100 == 0)
				System.out.println(count);
			count++;
		}
		buildFeature.computeThreshhold();
		buildFeature.buildBinaryLable();
		buildFeature.writeTrainFile("train.csv");
		buildFeature.writeTestFile("test.csv");

	}
}
