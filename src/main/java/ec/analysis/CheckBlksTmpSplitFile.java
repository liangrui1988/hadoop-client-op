package ec.analysis;

import ec.EcUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 指定hdfs路径文件(文件里面是的有文件列表),多线程并发检查文件是否是有损坏的EC文件，
 * 并把有损坏的EC文件保证到hdfs中 /user/hdev/ec_file_error/{month}
 * 例：
 * hadoop jar /home/liangrui/hdfs-client-op-1.0-SNAPSHOT.jar ec.analysis.CheckBlks "/user/hdev/dn_ec_reconstruct_map/dt=20240704/part-r-00000" "/user/hdev/dn_ec_reconstruct_map_status/dt=20240704/001.csv"
 **/

public class CheckBlksTmpSplitFile {

    public static StringBuffer sb = new StringBuffer();

    public static void main(String[] args) {
        String filePath = "";
        System.out.println("main args " + Arrays.toString(args));
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String day = dateFormat.format(new Date());

        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, -3);
        String remDate = dateFormat.format(calendar.getTime());
        System.out.println("remove path tmp day:"+remDate);

        if (args.length >= 1) {
            day = args[0];
        }
        if (args.length >= 2) {
            filePath = args[1];
        }

        if (StringUtils.isBlank(filePath)) {
            filePath = "/user/hdev/dn_ec_reconstruct_map/dt=" + day + "/part-r-00000";
        }

        System.out.println(filePath);

        BufferedReader reader = null;
        FSDataOutputStream outputStream = null;
        try {
            Configuration conf = new Configuration();
            Configuration conf2 = new Configuration();
            conf2.set("fs.defaultFS", "hdfs://yycluster06");
            conf2.set("hadoop.security.authentication", "kerberos");
            conf.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("hdev@YYDEVOPS.COM", "/home/liangrui/hdev.keytab");
            URLConnectionFactory connectionFactory = URLConnectionFactory.newDefaultURLConnectionFactory(conf);
            DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
            DistributedFileSystem fs2 = (DistributedFileSystem) FileSystem.get(conf2);
            //remove tmp dir
            fs.delete(new Path("/user/hdev/dn_ec_reconstruct_split_blk_tmp/dt=" + remDate),true);
            //reader file path list
            Path path = new Path(filePath);
            FSDataInputStream inputStream = fs.open(path);
            reader = new BufferedReader(new InputStreamReader(inputStream));
            //去重，write tmp
            Set<String> setList = new HashSet<>();
            int s = 0;
            String line = reader.readLine();
            while (line != null) {
                s++;
                line = reader.readLine();
                if (StringUtils.isBlank(line)) {
                    continue;
                }
                String[] lineArray = line.split(",");
                String blk = lineArray[2];
                setList.add(blk);
                if (setList.size() >= 20000) break;
            }
            reader.close();
            System.out.println("cont=" + s + " setList size=" + setList.size());
            //存储线程的返回值
            int index = 0;
            int fileNo = 0;
            for (String sdata : setList) {
                index++;
                sb.append(sdata).append("\n");
                if (index % 2000 == 0) {
                    fileNo++;
                    Path tmpWirteFile = new Path("/user/hdev/dn_ec_reconstruct_split_blk_tmp/dt=" + day + "/" + fileNo + ".txt");
                    outputStream = fs.create(tmpWirteFile, true);
                    outputStream.writeBytes(sb.toString());
                    outputStream.close();
                    sb = new StringBuffer();
                }
            }
            fileNo++;
            Path tmpWirteFile = new Path("/user/hdev/dn_ec_reconstruct_split_blk_tmp/dt=" + day + "/" + fileNo + ".txt");
            if (sb.length() > 0) {
                outputStream = fs.create(tmpWirteFile, true);
                outputStream.writeBytes(sb.toString());
                outputStream.close();
            } else {
                System.out.println("sb is null");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }


}

