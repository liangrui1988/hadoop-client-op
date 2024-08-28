package compress;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;

/**
 * text to Compress is mapreduce
 * creat by liangrui on 20240827
 */
public class CompressMr {

    private static Logger log = Logger.getLogger(CompressMr.class.getName());

    /**
     * @param args inputDir:
     *             outputDir:
     *             separator: hive table separator
     *             compressCodec: default org.apache.hadoop.io.compress.GzipCodec
     *             Given the corresponding input parameters, output the compressed result
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        System.out.println("main args " + Arrays.toString(args));
        String inputDir = "";
        String outputDir = "";
        String compressCodec = "org.apache.hadoop.io.compress.GzipCodec";
        String separator = "\t";
        if (args.length >= 1) {
            inputDir = args[0];
        }
        if (args.length >= 2) {
            outputDir = args[1];
        }
        if (args.length >= 3) {
            compressCodec = args[2];
        }
        if (args.length >= 4) {
            separator = args[3];
        }
        if (StringUtils.isBlank(inputDir) || StringUtils.isBlank(outputDir)) {
            System.err.println("args is blank");
            System.exit(0);
        }
        Configuration conf = new Configuration();
        String hdfsFS = inputDir.split("/hive_warehouse")[0];
        System.out.println("hdfsfs is" + hdfsFS);
        conf.set("fs.defaultFS", hdfsFS);
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("mapreduce.output.textoutputformat.separator", separator); // \001
        conf.set("mapreduce.job.map", "10");
        conf.set("mapreduce.tasktracker.map.tasks.maximum", "20");
        conf.set("mapreduce.tasktracker.reduce.tasks.maximum", "10");

        conf.set("mapreduce.job.running.map.limit", "200");
        conf.set("mapreduce.job.running.reduce.limit", "200");
        conf.set("mapreduce.reduce.cpu.vcores", "4");
        conf.set("mapreduce.map.memory.mb", "4096");
        conf.set("mapreduce.reduce.memory.mb", "8092");

        //set file compression
        conf.set("mapreduce.output.fileoutputformat.compress", "true");
        conf.set("mapreduce.output.fileoutputformat.compress.codec", compressCodec);
        DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
        ContentSummary contentSum = fs.getContentSummary(new Path(inputDir));
        long dirLength = contentSum.getLength();
        System.out.println(contentSum.toString());
        long reducesNum = 1;

        //set file size
        if ("org.apache.hadoop.io.compress.BZip2Codec".equals(compressCodec)) {
            //bzip2 Support split data file compression efficiency is better
            //It takes longer to compress
            conf.set("mapreduce.input.fileinputformat.split.minsize", "5368709120");//5g
            conf.set("mapreduce.input.fileinputformat.split.minsize.per.node", "5120000000");
            conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack", "5120000000");
            conf.set("mapreduce.input.fileinputformat.split.maxsize", "10737418240");
            if (dirLength > 5368709120L) {
                reducesNum = dirLength / 3 / 5368709120L; //%75 compression ratio
            }
        } else {
            //gzip  Compression time is faster and split is not supported
            //Fast compression time
            conf.set("mapreduce.input.fileinputformat.split.minsize", "268435456");
            conf.set("mapreduce.input.fileinputformat.split.minsize.per.node", "256000000");
            conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack", "256000000");
            conf.set("mapreduce.input.fileinputformat.split.maxsize", "536870912");
            if (dirLength > 536870912) {
                reducesNum = dirLength / 3 / 536870912;
            }
        }
        System.out.println("mapreduce.job.reduces set is:" + reducesNum);
        conf.set("mapreduce.job.reduces", String.valueOf(reducesNum));
        conf.set("mapreduce.job.cache.archives", hdfsFS + "/hdp/apps/3.1.0.0-78/mapreduce/mapreduce.tar.gz#mr-framework");
        conf.set("mapreduce.job.working.dir", hdfsFS + "/user/hdfs");
        conf.set("mapreduce.jobhistory.done-dir", hdfsFS + "/mr-history/done");
        conf.set("mapreduce.jobhistory.intermediate-done-dir", hdfsFS + "/mr-history/tmp");
        conf.set("mapreduce.job.inputformat.class", "org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat");

        fs.delete(new Path(outputDir), true);
        Job job = Job.getInstance(conf, "text-data-compress");

        job.setJarByClass(CompressMr.class);
        job.setMapperClass(CompressMapper.class);

        job.setReducerClass(CompressReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // FileInputFormat.setInputDirRecursive(job, true);
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.addInputPath(job, new Path(inputDir));
        //FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class CompressMapper
            extends Mapper<LongWritable, Text, Text, NullWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }

    public static class CompressReducer
            extends Reducer<Text, NullWritable, Text, NullWritable> {
        private Text result = new Text();
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            for (Text value : values) {
//                context.write(value, NullWritable.get());
//            }
            result.set(values.toString());
            context.write(result, NullWritable.get());
        }
    }

}