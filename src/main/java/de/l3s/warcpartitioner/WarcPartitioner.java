package de.l3s.warcpartitioner;

import de.l3s.concatgz.io.ImmediateOutput;
import de.l3s.concatgz.io.warc.WarcGzInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WarcPartitioner extends Configured implements Tool {
    public final static String TOOL_NAME = "WarcPartitioner";

    public final static short REPLICATION = 2;

    @Override
    public int run(String[] args) throws Exception {
        Configuration config = getConf();

        Job job = Job.getInstance(config);
        job.setJobName(TOOL_NAME);
        job.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(WarcGzInputFormat.class);

        job.setMapperClass(WarcPartitionMapper.class);

        ImmediateOutput.initialize(job);

        Path outPath = new Path(args[1]);
        ImmediateOutput.setPath(job, outPath);
        ImmediateOutput.setIdPrefix(job, outPath.getName());
        ImmediateOutput.setExtension(job, ".gz");
        ImmediateOutput.setReplication(job, REPLICATION);

        job.setNumReduceTasks(0);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static Configuration config() {
        Configuration config = new Configuration();
        config.setBoolean("mapreduce.map.speculative.execution", false);
        config.setBoolean("mapreduce.reduce.speculative.execution", false);
        config.setBoolean("mapreduce.output.fileoutputformat.compress", false);

        return config;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(config(), new WarcPartitioner(), args);
        System.exit(res);
    }
}
