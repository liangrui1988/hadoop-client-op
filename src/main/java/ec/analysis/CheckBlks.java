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
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import java.io.*;
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

public class CheckBlks {

    public static StringBuffer sb = new StringBuffer();
    public static StringBuffer ErrorBlk = new StringBuffer();

    public static List<String> filterDir;

    static {
        filterDir = new ArrayList<>();
        filterDir.add("hive_warehouse/projectstream.db");
        filterDir.add("hive_warehouse/project_stream_pwc.db");
        filterDir.add("hive_warehouse/onepiece.db");
        filterDir.add("hive_warehouse/biugolite.db");
        filterDir.add("hive_warehouse/bigo_export.db");
        filterDir.add("hive_warehouse/warehouse_old_snapshots");
        filterDir.add("hive_warehouse/hdfs-archive");
        filterDir.add("hive_warehouse/dw_oversea_pub.db");
        filterDir.add("hive_warehouse/text_error_back");
        filterDir.add("hive_warehouse/hive_warehouse_repl");
        filterDir.add("hive_warehouse/recover");
        filterDir.add("hive_warehouse/error_back2");
        filterDir.add("hive_warehouse/error_back");
    }

    public static void main(String[] args) {
        String filePath = "";
        String outPath = "";
        System.out.println("main args " + Arrays.toString(args));
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String day = dateFormat.format(new Date());
        String fileNo = "1";
        if (args.length >= 1 && StringUtils.isNotBlank(args[0])) {
            fileNo = args[0];
        }
//        if (args.length >= 1 && StringUtils.isNotBlank(args[0])) {
//            day = args[0];
//        }
        if (args.length >= 2) {
            filePath = args[1];
        }
        if (args.length >= 3) {
            outPath = args[2];
        }

        if (StringUtils.isBlank(filePath)) {
            filePath = "/user/hdev/dn_ec_reconstruct_split_blk_tmp/dt=" + day + "/" + fileNo + ".txt";
        }
        if (StringUtils.isBlank(outPath)) {
            outPath = "/user/hdev/dn_ec_reconstruct_map_status/dt=" + day + "/" + fileNo + ".csv";
        }
        System.out.println(filePath);
        System.out.println(outPath);
        Path wirteFile = new Path(outPath);
        ExecutorService executorService0 = Executors.newFixedThreadPool(1);

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
                // String[] lineArray = line.split(",");
                //String blk = lineArray[2];
                setList.add(line);
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
                    executorService0.submit(task);
                    results.clear();
                }
            }
            if (results.size() > 0) {
                RunTabThread2 task = new RunTabThread2(fs, fs2, results, connectionFactory);
                System.out.println("executorService0,start size:" + results.size());
                executorService0.submit(task);
            }

            executorService0.shutdown();
            try {//等待直到所有任务完成
                System.out.println("executorService.start()===awaitTermination");
                executorService0.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//            while (executorService.isShutdown()) {
//                System.out.println("executorService.isShutdown()===" + executorService.isShutdown());
//                if (executorService.isShutdown()) {
//                    System.out.println("break executorService.isShutdown()===" + executorService.isShutdown());
//                    break;
//                }
//            }
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
        if (ErrorBlk.length() > 1) {   //发送告警消息
            String msg = "{\"iid\":\"45496\",\"sid\":\"367116\",\"message\": \"ecBlkError\",\"msg_key\":\"801\"}";
            try {
                snedPost("http://prometheus-send-alter.yy.com/pushPhone", msg);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void snedPost(String url, String json) throws Exception {
        System.out.println(url);
        System.out.println(json);
        String charset = "UTF-8";
        URLConnection connection = new URL(url).openConnection();
        connection.setDoOutput(true); // Triggers POST.
        connection.setRequestProperty("Accept-Charset", charset);
        connection.setRequestProperty("Content-Type", "application/json;charset=" + charset);
        try (OutputStream output = connection.getOutputStream()) {
            output.write(json.getBytes(charset));
        }
        InputStream response = connection.getInputStream();
        System.out.println(response.toString());
        response.close();
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
                        String newLine = blk + ",fpNull," + hosts + ",," + time;
                        sb.append(newLine).append("\n");
                        continue;
                    }
                    String[] lineArray = filePath.split("/");
                    if (lineArray.length > 2) {
                        String f = lineArray[1] + "/" + lineArray[2];
                        if (filterDir.contains(f)) {
                            System.out.println("skip filePath is:" + filePath + " ," + hosts);
                            continue;
                        }
                    }
                    EcUtils eu = new EcUtils();
                    Map<String, String> euResult = eu.checkEC(filePath, dfs2);
                    String blkStatus = euResult.get("status");
                    //System.out.println("msg===" + euResult.get("msg"));
                    String newLine = blk + "," + filePath + "," + hosts + "," + blkStatus + "," + time;
                    sb.append(newLine).append("\n");
                    if (!"0".equals(blkStatus)) {
                        ErrorBlk.append(newLine);
                    }
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

