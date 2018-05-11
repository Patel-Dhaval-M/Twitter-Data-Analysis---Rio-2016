import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.Hashtable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopSportsMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Hashtable<String, String> athleteInfo;
    	private final IntWritable one = new IntWritable(1);
    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		int noOfSemicolons = value.toString().replaceAll("[^;]","").length();
		if (noOfSemicolons == 3) {
			String[] tweet = value.toString().split(";");
			for (String athlete : athleteInfo.keySet()) {
				if (athlete != null) {
					if (tweet[2].toLowerCase().contains(athlete.toLowerCase())) {
						context.write(new Text(athleteInfo.get(athlete)), one);
					}
				}
			}
		}
    	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

                athleteInfo = new Hashtable<String, String>();

		// We know there is only one cache file, so we only retrieve that URI
		URI fileUri = context.getCacheFiles()[0];

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try {
			// we discard the header row
			br.readLine();

			while ((line = br.readLine()) != null) {
				context.getCounter(AthleteCounters.NUM_ATHLETES).increment(1);

				String[] fields = line.split(",");
				if (fields.length == 11) {
					athleteInfo.put(fields[1], fields[7]);
                                }
			}
			br.close();
		} catch (IOException e1) {
		}
		super.setup(context);
	}
}
