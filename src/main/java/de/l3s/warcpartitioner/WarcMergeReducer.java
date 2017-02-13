package de.l3s.warcpartitioner;

import com.google.common.io.ByteSource;
import de.l3s.concatgz.io.ConcatGzipInputFormat;
import de.l3s.concatgz.util.GZipBytes;
import de.l3s.warcpartitioner.utils.WarcHeaders;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class WarcMergeReducer extends Reducer<Text, Text, NullWritable, NullWritable> {
    private FileSystem fs;
    private int bufferSize;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        fs = FileSystem.newInstance(context.getConfiguration());
        bufferSize = context.getConfiguration().getInt("io.file.buffer.size", 4096);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        try {
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        GZipBytes gzip = new GZipBytes();

        ConcatGzipInputFormat.ConcatGzipRecordReader reader = new ConcatGzipInputFormat.ConcatGzipRecordReader();

        String[] segments = key.toString().split("\\/");
        String type = segments[0];
        String year = segments[1];
        String warcOrArc = segments[2];

        long currentOutSize = 0;
        int outFileCount = 0;
        OutputStream currentOut = null;
        for (Text value : values) {
            context.progress();

            Path path = new Path(value.toString());
            long fileLength = -1;
            try {
                fileLength = fs.getFileStatus(path).getLen();
            } catch (Exception e) {
                continue;
            }

            boolean split = currentOutSize + fileLength > WarcMerger.MAX_FILESIZE;
            if (split) reader.initialize(fs.open(path), path.getName());

            while (!split || reader.nextKeyValue()) {
                context.progress();

                ByteSource bytes = null;
                long length = -1;
                if (split) {
                    bytes = reader.getCurrentValue().getBytes();
                    length = bytes.size();
                }

                if (currentOut == null || currentOutSize + length > WarcMerger.MAX_FILESIZE) {
                    if (currentOut != null) {
                        currentOut.flush();
                        currentOut.close();
                    }

                    String outFile = outFileCount + "";
                    while (outFile.length() < 5) outFile = "0" + outFile;
                    outFile = type + "/" + year + "/" + outFile + "." + warcOrArc + ".gz";
                    Path outPath = new Path(WarcMerger.getOutDir(context), outFile);

                    fs.mkdirs(outPath.getParent());
                    currentOut = fs.create(outPath, true, bufferSize, WarcMerger.REPLICATION, WarcMerger.MAX_FILESIZE);
                    byte[] header = warcOrArc.equals("arc") ? WarcHeaders.getArcHeader(outPath.getName()) : WarcHeaders.getWarcHeader(outPath.getName());
                    gzip.open().write(header);
                    byte[] gzipHeader = gzip.close();
                    currentOut.write(gzipHeader);
                    currentOutSize = gzipHeader.length;

                    outFileCount++;
                }

                if (split) {
                    InputStream in = bytes.openStream();
                    IOUtils.copy(in, currentOut);
                    in.close();
                    currentOutSize += length;
                } else {
                    InputStream in = fs.open(path);
                    IOUtils.copy(in, currentOut);
                    in.close();
//                    fs.delete(path, false);
                    currentOutSize += fileLength;
                }

                if (!split) break;
            }

            if (split) {
                reader.close();
//                fs.delete(path, false);
            }
        }

        if (currentOut != null) {
            currentOut.flush();
            currentOut.close();
        }
    }
}
