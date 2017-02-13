package de.l3s.warcpartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class WarcMerger extends Configured implements Tool {
    public final static String TOOL_NAME = "WarcMerger";

    public final static int REDUCERS_YEARS_FACTOR = 18;
    public final static long MAX_FILESIZE = 1024 * 1024 * 1024; // 1 GB
    public final static short REPLICATION = 2;

    @Override
    public int run(String[] args) throws Exception {
        Configuration config = getConf();

        Job job = Job.getInstance(config);
        job.setJobName(TOOL_NAME);
        job.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(NLineInputFormat.class);

        job.setMapperClass(WarcMergeMapper.class);
        job.setReducerClass(WarcMergeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputFormatClass(NullOutputFormat.class);
        setOutDir(job, new Path(args[1]));

        Set<String> partitions = new HashSet<>(WarcPartitionMapper.typeMap.values());
        for (String typeClass : WarcPartitionMapper.typeClasses) {
            partitions.add(typeClass + "_" + WarcPartitionMapper.miscKey);
        }
        partitions.add(WarcPartitionMapper.miscKey);
        job.setNumReduceTasks(REDUCERS_YEARS_FACTOR * partitions.size() * 2); // * 2 for (arcs + warcs)

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static Configuration config() {
        Configuration config = new Configuration();
        config.setBoolean("mapreduce.map.speculative.execution", false);
        config.setBoolean("mapreduce.reduce.speculative.execution", false);
        config.setBoolean("mapreduce.output.fileoutputformat.compress", false);

        return config;
    }

    public static final String OUT_DIR = "de.l3s.warcpartitioner.outdir";

    public static void setOutDir(Job job, Path path) throws IOException {
        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        path = fs.makeQualified(path);
        String dirStr = StringUtils.escapeString(path.toString());
        conf.set(OUT_DIR, dirStr);
    }

    public static Path getOutDir(JobContext context) {
        return new Path(StringUtils.unEscapeString(context.getConfiguration().get(OUT_DIR)));
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(config(), new WarcMerger(), args);
        System.exit(res);
    }
}
