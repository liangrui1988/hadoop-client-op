package ec;

import com.yy.bigdata.WordCount;
import com.yy.bigdata.utils.HdfsCUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collectors;


/**
 * 切分任务,把目录下的所有文本，按行数切分为10个文件，每个机器取一个文件去执行
 *
 * hadoop jar hdfs-client-op-1.0-SNAPSHOT.jar ec.AllFileSplit
 */
public class AllFileSplit {


    public static void main(String[] args) throws Exception {
        String inputDir = "/user/hdev/ec_file/";
        String outputDir = "/user/hdev/ec_file_split/";
        System.out.println("main args " + Arrays.toString(args));
        if (args.length >= 1) {
            inputDir = args[0];
        }
        if (args.length >= 2) {
            outputDir = args[2];
        }
        Configuration conf = HdfsCUtils.getCfg();
        DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
        UserGroupInformation.setConfiguration(conf);
        RemoteIterator<LocatedFileStatus> fileList = fs.listFiles(new Path(inputDir), true);
        List<String> allFile = new ArrayList<String>();
        while (fileList.hasNext()) {
            LocatedFileStatus lfs = fileList.next();
            //String path=lfs.getPath().toUri().getPath();
            FSDataInputStream inputStream = fs.open(lfs.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line = reader.readLine();
            while (line != null) {
                line = reader.readLine();
                if (StringUtils.isBlank(line)) {
                    continue;
                }
                allFile.add(line.trim());
            }
        }
        if (allFile.size() <= 0) {
            System.out.println("size null exit");
            System.exit(1);
        }
        int fileCount = allFile.size() ;
        System.out.println("file count ====" + fileCount);
        int nodeSize = 9;
        int limit = fileCount / nodeSize;
//        List<String> node0 = allFile.subList(0*0, limit*1);
//        List<String> node1 = allFile.subList(limit*1, limit * 2);
//        List<String> node2 = allFile.subList(limit * 2, limit * 3);
//        List<String> node3 = allFile.subList(limit * 3, limit * 4);
//        List<String> node4 = allFile.subList(limit * 4, limit * 5);
//        List<String> node5 = allFile.subList(limit * 5, limit * 6);
//        List<String> node6 = allFile.subList(limit * 6, limit * 7);
//        List<String> node7 = allFile.subList(limit * 7, limit * 8);
//        List<String> node8 = allFile.subList(limit * 8, limit * 9);
//        List<String> nodeLast = allFile.subList(limit * nodeSize, allFile.size());
        //System.out.println(nodeLast);
        for (int i = 0; i <= nodeSize; ++i) {
            List<String> tmpNode = null;
            if (i == nodeSize) {
                tmpNode = allFile.subList(limit * i, fileCount);
            } else {
                tmpNode = allFile.subList(limit * i, limit * (i + 1));
            }
            StringBuilder sb = new StringBuilder();
            for (String s : tmpNode) {
                sb.append(s).append("\n");
            }
            String outpath=outputDir + i + "_file_list.txt";
            System.out.println("outpath is:"+outpath);
            System.out.println("outpath count is:"+tmpNode.size());
            Path wirteFile = new Path(outputDir + i + "_file_list.txt");
            FSDataOutputStream outputStream = fs.create(wirteFile, true);
            outputStream.writeBytes(sb.toString());
            outputStream.close();
        }


    }

}
