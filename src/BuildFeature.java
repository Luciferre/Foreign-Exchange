import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;

import javax.xml.crypto.Data;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class BuildFeature {
	// array: time, bid, ask, minutes, Max, Min, Mean, difference, lable
	static ArrayList<ArrayList<String>> data = new ArrayList<>();

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

	public float compute(int index, int minutes, float lastBid) {
		float max = Float.MIN_VALUE;
		float min = Float.MAX_VALUE;
		float mean = 0, diff = 0;
		int lable = 0;
		ArrayList<String> base = data.get(index);
		DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMMdd HH:mm:ss.SSS");
		DateTime baseDateTime = formatter.parseDateTime(base.get(0));
		DateTime previousDateTime = baseDateTime.minusMinutes(minutes);

		int count = 0;
		for (int i = index; i >= 0; i--) {
			ArrayList<String> tmpList = data.get(i);
			DateTime tmpDateTime = formatter.parseDateTime(tmpList.get(0));
			if (tmpDateTime.isBefore(previousDateTime))
				break;

			float tmpbid = Float.parseFloat(tmpList.get(1));
			if (max < tmpbid)
				max = tmpbid;
			if (min > tmpbid)
				min = tmpbid;
			mean += tmpbid;
			count++;
		}
		float bid = Float.parseFloat(base.get(1));
		float ask = Float.parseFloat(base.get(2));

		diff = ask - bid;
		if (lastBid != 0) {
			if (bid >= lastBid)
				lable = 1;
			else
				lable = 0;
		}
		base.add(String.valueOf(baseDateTime.getMinuteOfDay()));
		base.add(String.format("%.5f", max));
		base.add(String.format("%.5f", min));
		base.add(String.format("%.5f", mean / count));
		base.add(String.format("%.5f", diff));
		base.add(String.valueOf(lable));

		return bid;
	}

	public void writeFile(String fileName) {
		File file = new File(fileName);
		try {
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			for (int i = 0; i < data.size(); i++) {
				ArrayList<String> tmpArrayList = data.get(i);
				for (int j = 3; j < tmpArrayList.size(); j++) {
					if (j == 3)
						bw.write(tmpArrayList.get(j));
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
		BuildFeature buildFeature = new BuildFeature();
		buildFeature.readFile("sample.csv");
		int count = 0;
		float lastBid = 0;
		for (int i = 0; i < data.size(); i++) {
			lastBid = buildFeature.compute(i, 5, lastBid);
			if (count % 100 == 0)
				System.out.println(count);
			count++;
		}
		buildFeature.writeFile("result.csv");

	}
}
