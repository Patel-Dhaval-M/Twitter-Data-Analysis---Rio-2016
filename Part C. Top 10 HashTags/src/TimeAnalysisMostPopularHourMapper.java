import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class TimeAnalysisMostPopularHourMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private Text tweet = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        int noOfSemicolons = value.toString().replaceAll("[^;]","").length();
        if (noOfSemicolons == 3) {
            String[] fields = value.toString().split(";");
            try {
                long timestamp = Long.parseLong(fields[0]);
                Date dt = new Date(timestamp);
                if (dt.getHours() == 2) {
                    Pattern MY_PATTERN = Pattern.compile("#(\\S+)");
                    Matcher mat = MY_PATTERN.matcher(fields[2]);
                    while (mat.find()) {
                        tweet.set(mat.group(1).toLowerCase());
                        context.write(tweet, one);
                    }
                }
            }
            catch(NumberFormatException ex){ // handle your exception
                System.out.println("not a number");
            }
        }
    }
}
