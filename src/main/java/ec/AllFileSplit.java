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
 * 切分任务
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
        StringBuilder sb = new StringBuilder();
        List<String> allFile=new ArrayList<String>();
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
        int splitCount=allFile.size()/10;
        System.out.println("file count ===="+allFile.size());
        allFile.subList(1,splitCount);


    }

}
