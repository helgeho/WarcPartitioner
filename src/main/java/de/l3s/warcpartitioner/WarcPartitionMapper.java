package de.l3s.warcpartitioner;

import de.l3s.concatgz.data.WarcRecord;
import de.l3s.concatgz.io.ImmediateOutput;
import de.l3s.concatgz.io.warc.WarcWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class WarcPartitionMapper extends Mapper<NullWritable, WarcWritable, NullWritable, NullWritable> {
    public static final Map<String, String> typeMap = new HashMap<>();
    public static final Set<String> typeClasses = new HashSet<>();
    public static final String miscKey = "misc";

    static {
        typeMap.put("text/html", "html");
        typeMap.put("text/xml", "xml");
        typeMap.put("application/xhtml+xml", "xml");
        typeMap.put("text/plain", "plaintext");
        typeMap.put("text/css", "css");
        typeMap.put("text/javascript", "javascript");
        typeMap.put("text/x-javascript", "javascript");
        typeMap.put("application/javascript", "javascript");
        typeMap.put("application/x-javascript", "javascript");
        typeMap.put("application/x-shockwave-flash", "flash");
        typeMap.put("application/pdf", "pdf");
        typeMap.put("image/jpeg", "jpeg");
        typeMap.put("image/jpg", "jpeg");
        typeMap.put("image/gif", "gif");
        typeMap.put("image/tiff", "tiff");
        typeMap.put("image/png", "png");

        typeClasses.add("text");
        typeClasses.add("image");
        typeClasses.add("audio");
        typeClasses.add("video");
        typeClasses.add("application");
    }

    private ImmediateOutput output;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        output = new ImmediateOutput(context, false);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        try {
            output.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(NullWritable key, WarcWritable value, Context context) throws IOException, InterruptedException {
        WarcRecord record = value.getRecord();

        String type = record.getHttpMimeType();
        if (type == null) type = "";
        type = type.toLowerCase();

        if (type.equals("warc-info")) return; // skip warc headers

        String recordType = value.getFilename().toLowerCase().matches(".*\\.warc(\\.[^\\.]+$|$)") ? "warc" : "arc";

        String partitionKey;
        if (typeMap.containsKey(type)) {
            partitionKey = typeMap.get(type);
        } else {
            String[] typeSegments = type.split("\\/");
            String typeClass = typeSegments.length == 0 ? "" : typeSegments[0];
            if (typeClasses.contains(typeClass)) {
                partitionKey = typeClass + "_" + miscKey;
            } else {
                partitionKey = miscKey;
            }
        }

        int year;
        try {
            String date = record.getHeader().getDate();
            year = Integer.parseInt(date.substring(0, 4));
        } catch (Exception e) {
            e.printStackTrace();
            year = 0;
        }
        partitionKey += "/" + year + "/" + recordType;

        OutputStream out = output.stream(partitionKey);
        value.getBytes().copyTo(out);
        out.flush();
    }
}
