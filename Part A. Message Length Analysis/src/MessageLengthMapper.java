import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MessageLengthMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        int noOfSemicolons = value.toString().replaceAll("[^;]","").length();
        if (noOfSemicolons == 3) {
            String[] fields = value.toString().split(";");
            String outKey = "";
            if (fields[2].length() > 0 && fields[2].length() <= 140) {
	             for (int i=5; i<=140; i=i+5) {
                  if (i >= fields[2].length()) {
	                  outKey=(i-4)+"-"+(i);
                    context.write(new Text(outKey), one);
                    break;
                  }
                }
            }
        }
    }
}
