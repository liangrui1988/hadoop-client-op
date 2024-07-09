package ec.analysis;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.logging.Logger;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * ec log  analyse blockId to file path
 * <p>
 * -Dmapreduce.job.maps=10
 * export HADOOP_OPTS="-Dsun.security.krb5.debug=true"
 * hadoop jar  hdfs-client-op-1.0-SNAPSHOT.jar  ec.analysis.LogBlockToFile /user/hdev/dn_ec_reconstruct/20240704/*
 * hadoop jar  hdfs-client-op-1.0-SNAPSHOT.jar  ec.analysis.LogBlockToFile /user/hdev/dn_ec_reconstruct/20240707/* /user/hdev/dn_ec_reconstruct_map/dt=20240707
 * <p>
 * bin/hadoop jar yourapp.jar ... -Dmapreduce.job.maps=5
 */
public class LogBlockToFile {

    private static Logger log = Logger.getLogger(LogBlockToFile.class.getName());

    static String ECReconstruct = "EC reconstruct striped";
    static String FailedToreconstruct = "Failed to reconstruct";
    static String InvalidDecodingException = "InvalidDecodingException";

    public static void main(String[] args) throws Exception {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String cday = dateFormat.format(new Date());
        String inputDir = "/user/hdev/dn_ec_reconstruct/" + cday + "/*";
        String outputDir = "/user/hdev/dn_ec_reconstruct_map/dt=" + cday;
        Configuration conf = new Configuration();
        // Configuration conf = HdfsCUtils.getCfg();
        conf.set("fs.defaultFS", "hdfs://yycluster01");
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        //conf.set("mapreduce.job.map", "2");
        conf.set("mapreduce.tasktracker.map.tasks.maximum", "10");
        conf.set("mapreduce.job.running.map.limit", "50");
//        conf.set("mapreduce.job.cache.archives", "hdfs://yycluster06/hdp/apps/3.1.0.0-78/mapreduce/mapreduce.tar.gz#mr-framework");
//        conf.set("mapreduce.job.working.dir", "hdfs://yycluster06/user/hdfs");
//        conf.set("mapreduce.jobhistory.done-dir", "hdfs://yycluster06/mr-history/done");
//        conf.set("mapreduce.jobhistory.intermediate-done-dir", "hdfs://yycluster06/mr-history/tmp");
//        conf.set("mapreduce.input.lineinputformat.linespermap", "10000");

        System.out.println("main args " + Arrays.toString(args));
        if (args.length >= 1) {
            inputDir = args[0];
        }
        if (args.length >= 2) {
            outputDir = args[1];
        }
        DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
        fs.delete(new Path(outputDir), true);
        Job job = Job.getInstance(conf, "ec log ETL");
        job.setJarByClass(LogBlockToFile.class);
        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(TextReducer.class);
        // job.setReducerClass(TextReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setInputFormatClass(NLineInputFormat.class);
        FileInputFormat.setMinInputSplitSize(job, 268435456);
        FileInputFormat.setMaxInputSplitSize(job, 268435456 * 2);
        FileInputFormat.setInputDirRecursive(job, true);
//        MultipleOutputs.addNamedOutput(job, "a", TextOutputFormat.class, Text.class, Text.class);
//        MultipleOutputs.addNamedOutput(job, "b", TextOutputFormat.class, Text.class, Text.class);
        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private final Text newLine = new Text();
        private final Text keyName = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // StringTokenizer itr = new StringTokenizer(value.toString());
            String valueStr = value.toString();
            if (!StringUtils.isBlank(valueStr) && !valueStr.contains("Binary file")) {
                //System.out.println("map line: " + valueStr);
                //split get file path
                String blkid = "";
                String keyStr = "";
                if (valueStr.contains(InvalidDecodingException)) {
                    keyStr = "invalidDecoding";
                    String[] defaults = valueStr.split("blk_");
                    if (defaults.length != 2) {
                        System.err.println("defaults  not -blk_-: " + valueStr);
                    }
                    blkid = valueStr.split("blk_")[1].split("_")[0].trim();
                    blkid = "blk_" + blkid;

                } else if (valueStr.contains(ECReconstruct)) {
                    keyStr = "reconstruct";
                    String[] reconstructs = valueStr.split("blockId:");
                    if (reconstructs.length != 2) {
                        System.err.println("reconstructs  not blockId:: " + valueStr);
                    }
                    blkid = valueStr.split("blockId:")[1].trim();

                } else if (valueStr.contains(FailedToreconstruct)) {
                    keyStr = "failed";
                    String[] defaults = valueStr.split("blk_");
                    if (defaults.length != 2) {
                        System.err.println("defaults  not -blk_-: " + valueStr);
                    }
                    blkid = valueStr.split("blk_")[1].split("_")[0].trim();
                    blkid = "blk_" + blkid;
                }

                //dt
                String dtime = valueStr.split(":202")[1].substring(0, 16);
                String[] hostnames = valueStr.split(".com.log")[0].split("-datanode-");
                String hostname = "";
                if (hostnames.length != 2) {
                    System.err.println("map hostnames not -datanode-: " + valueStr);
                } else {
                    hostname = hostnames[1].trim() + ".com";
                }
                keyName.set(keyStr);
                String newFormat = hostname + "," + blkid + ",202" + dtime;
                //System.out.println("map  newFormat: " + newFormat);
                newLine.set(newFormat);
                context.write(keyName, newLine);
            }

        }
    }

    public static class TextReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
//        private MultipleOutputs outputs;
//
//
//        @Override
//        public void setup(Context context) throws IOException, InterruptedException {
//            System.out.println("enter TextReducer:::setup method");
//            outputs = new MultipleOutputs(context);
//        }
//
//        @Override
//        public void cleanup(Context context) throws IOException, InterruptedException {
//            System.out.println("enter TextReducer:::cleanup method");
//            outputs.close();
//        }

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            StringBuilder sb = new StringBuilder();
//            for (Text tv : values) {
//                String valueStr = tv.toString();
//                if (StringUtils.isBlank(valueStr) || valueStr.contains("Binary file")) {
//                    System.err.println("null or continue valueStr is Binary file: " + valueStr);
//                    continue;
//                }
//                System.out.println("reduce valueStr: " + valueStr);
//                String[] hostnames = valueStr.split(".com.log")[0].split("-datanode-");
//                String hostname = "";
//                if (hostnames.length != 2) {
//                    System.err.println("reduce hostnames not -datanode-: " + valueStr);
//                } else {
//                    hostname = hostnames[1].trim() + ".com";
//                }
//                String blkid = "";
//                switch (key.toString()) {
//                    case "reconstruct":
//                        String[] reconstructs = valueStr.split("blockId:");
//                        if (reconstructs.length != 2) {
//                            System.err.println("reconstructs  not blockId:: " + valueStr);
//                            break;
//                        }
//                        blkid = valueStr.split("blockId:")[1].trim();
//                        break;
//                    default:
//                        String[] defaults = valueStr.split("blk_");
//                        if (defaults.length != 2) {
//                            System.err.println("defaults  not -blk_-: " + valueStr);
//                            break;
//                        }
//                        blkid = valueStr.split("blk_")[1].split("_")[0].trim();
//                        blkid = "blk_" + blkid;
//                        break;
//                }
//                String newFormat = key.toString() + "," + hostname + "," + blkid;
//                System.out.println("reduce newFormat: " + newFormat);
//                sb.append(newFormat);
//                sb.append("\r");
//            }
//            System.out.println("sb.toString()=====");
//            System.out.println(sb.toString());
//            result.set(sb.toString());
            result.set(values.toString());
            context.write(key, result);
        }
    }

}