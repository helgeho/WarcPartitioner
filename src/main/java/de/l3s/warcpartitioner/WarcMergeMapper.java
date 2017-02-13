package de.l3s.warcpartitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WarcMergeMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text keyOut = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String path = value.toString();
        String[] segments = path.split("\\/");
        int length = segments.length;
        if (length > 3) {
            String file = segments[length - 1];
            String year = segments[length - 2];
            String type = segments[length - 3];
            String[] fileSegments = file.split("-");
            String warcOrArc = fileSegments[0].toLowerCase();
            keyOut.set(type + "/" + year + "/" + warcOrArc);
            context.write(keyOut, value);
        }
    }
}
