package ec.analysis;

import com.yy.bigdata.utils.HdfsCUtils;
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
import org.apache.hadoop.security.authentication.client.AuthenticationException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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

public class CheckBlks {

    public static StringBuilder sb = new StringBuilder();

    public static void main(String[] args) {
        String filePath = "";
        String outPath = "";
        System.out.println("main args " + Arrays.toString(args));
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String day = dateFormat.format(new Date());

        if (args.length >= 1) {
            filePath = args[0];
        } else {
            filePath = "/user/hdev/dn_ec_reconstruct_map/dt=" + day + "/part-r-00000";
        }
        if (args.length >= 2) {
            outPath = args[1];
        } else {
            outPath = "/user/hdev/dn_ec_reconstruct_map_status/dt=" + day + "/001.csv";
        }

        Path wirteFile = new Path(outPath);
        ExecutorService executorService = Executors.newFixedThreadPool(90);
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

            //reader file path list
            Path path = new Path(filePath);
            FSDataInputStream inputStream = fs.open(path);
            reader = new BufferedReader(new InputStreamReader(inputStream));
            //去重
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
            }
            reader.close();
            System.out.println("cont=" + s);
            //存储线程的返回值
            List<String> results = new ArrayList<>();
            for (String sdata : setList) {
                results.add(sdata);
                if (results.size() >= 1000) {// limit 100
                    List<String> limitList = results.stream().map(e -> e).collect(Collectors.toList());
                    RunTabThread2 task = new RunTabThread2(fs, fs2, limitList, connectionFactory);
                    executorService.submit(task);
                    results.clear();
                }
            }
            if (results.size() > 0) {
                RunTabThread2 task = new RunTabThread2(fs, fs2, results, connectionFactory);
                executorService.submit(task);
            }
            executorService.shutdown();
            try {//等待直到所有任务完成
                System.out.println("executorService.start()===awaitTermination");
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            while (executorService.isShutdown()) {
                System.out.println("executorService.isShutdown()===" + executorService.isShutdown());
                if (executorService.isShutdown()) {
                    System.out.println("break executorService.isShutdown()===" + executorService.isShutdown());
                    break;
                }
            }
            if (sb.length() > 0) {
                outputStream = fs.create(wirteFile, true);
                outputStream.writeBytes(sb.toString());
            } else {
                System.out.println("sb is null");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
                if (outputStream != null) {
                    outputStream.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        String endTime = new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date());
        System.out.println(endTime + "=======all end==============");
    }

    public static class RunTabThread2 implements Runnable {
        private final DistributedFileSystem dfs;
        private final DistributedFileSystem dfs2;

        private List<String> limitList;
        private URLConnectionFactory connectionFactory;

        public RunTabThread2(DistributedFileSystem dfs, DistributedFileSystem fs2, List<String> limitList, URLConnectionFactory connectionFactory) {
            this.dfs = dfs;
            this.dfs2 = fs2;
            this.limitList = limitList;
            this.connectionFactory = connectionFactory;
        }

        public void run() {
            try {

                for (String blk : limitList) {
                    Map<String, Object> result = BlkUtils.doWork(dfs.getConf(), "blk_" + blk, connectionFactory);
                    //System.out.println("hello+++" + blk);
                    SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
                    String time = dateFormat.format(new Date());
                    String filePath = (String) result.get("filePath");
                    List<String> datanodes = (List<String>) result.get("datanodes");
                    String hosts = String.join("_", datanodes);
                    if (StringUtils.isBlank(filePath)) {
                        String newLine = blk + ",fpNull," + hosts+ ",,"+ time;
                        sb.append(newLine).append("\n");
                        continue;
                    }
                    EcUtils eu = new EcUtils();
                    Map<String, String> euResult = eu.checkEC(filePath, dfs2);
                    String blkStatus = euResult.get("status");
                    //System.out.println("msg===" + euResult.get("msg"));
                    String newLine = blk + "," + filePath + "," + hosts + "," + blkStatus + "," + time;
                    sb.append(newLine).append("\n");
                }
            } catch (Exception e) {
                System.err.println("error :" + e.getMessage());
                e.printStackTrace();
            } finally {
                System.gc();
            }
        }


    }
}
