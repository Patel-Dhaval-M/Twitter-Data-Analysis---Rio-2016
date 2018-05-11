import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Date;

public class TimeAnalysisMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        int noOfSemicolons = value.toString().replaceAll("[^;]","").length();
        if (noOfSemicolons == 3) {
            String[] fields = value.toString().split(";");
            try {
                long timestamp = Long.parseLong(fields[0]);
                Date dt = new Date(timestamp);
                context.write(new IntWritable(dt.getHours()), one);
            }
            catch(NumberFormatException ex){ // handle your exception
                System.out.println("not a number");
            }
        }
    }
}
