package LogCounter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (line.contains("[INFO]")) {
            word.set("INFO");
            context.write(word, one);
        } else if (line.contains("[SEVERE]")) {
            word.set("SEVERE");
            context.write(word, one);
        } else if (line.contains("[WARN]")) {
            word.set("WARN");
            context.write(word, one);
        }
    }
}
