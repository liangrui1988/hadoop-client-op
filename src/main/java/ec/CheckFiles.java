package ec;

import com.yy.bigdata.utils.HdfsCUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.AsyncAppender;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * hdfs dfs -touch hdfs://yycluster06/hive_warehouse/ec_check_list/hiidodw_db_yy_pcsdkprotocol_original
 * hdfs dfs -touch hdfs://yycluster06/hive_warehouse/ec_check_list/hiidosdk_db_yy_mbsdkevent_hour_original
 * hadoop jar /home/liangrui/hdfs-client-op-1.0-SNAPSHOT.jar ec.CheckFiles "/user/hdev/tmp_test.txt"
 * <p>
 * nohup  hadoop jar /home/liangrui/hdfs-client-op-1.0-SNAPSHOT.jar ec.CheckFiles "/user/hdev/sigle_ec_file/yy_mbsdkevent_hour_original.txt" > run_yy_mbsdkevent_hour_original.log 2>&1 &
 * nohup  hadoop jar /home/liangrui/hdfs-client-op-1.0-SNAPSHOT.jar ec.CheckFiles "/user/hdev/ec_file/yy_pcsdkprotocol_original.txt" > yy_pcsdkprotocol_original.log 2>&1 &
 * <p>
 * nohup  hadoop jar /home/liangrui/hdfs-client-op-1.0-SNAPSHOT.jar ec.CheckFiles /user/hdev/ec_file/yy_yysignalserviceboradcast_original.txt  > yy_yysignalserviceboradcast_original.log 2>&1 &
 * <p>
 * 10.12.67.127
 * nohup  hadoop jar /home/liangrui/hdfs-client-op-1.0-SNAPSHOT.jar ec.CheckFiles /user/hdev/ec_file/yy_yyabtestactivateact_original.txt  > yy_yyabtestactivateact_original.log 2>&1 &
 * --容器上跑的
 * nohup  hadoop jar /tmp/hdfs-client-op-1.0-SNAPSHOT.jar ec.CheckFiles /user/hdev/ec_file/yy_webyyanalysish5_original.txt  > /data/yy_webyyanalysish5_original.log 2>&1 &
 **/

public class CheckFiles {


    public static StringBuilder sb = new StringBuilder();

    public static void main(String[] args) {
        String filePath = "";
        System.out.println("main args " + Arrays.toString(args));
        if (args.length >= 1) {
            filePath = args[0];
        } else {
            System.exit(0);
        }

        int slash = filePath.lastIndexOf("/");
        String saveName = filePath.substring(slash + 1);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMM");
        String day = dateFormat.format(new Date());
        Path wirteFile = new Path("/user/hdev/ec_file_error/" + day + "/" + saveName + "_error.txt");
        Path not_error_wirteFile = new Path("/user/hdev/ec_file_error/" + day + "/" + saveName + "_not_err.succeed");
        ExecutorService executorService = Executors.newFixedThreadPool(150);
        // BlockingQueue<java.lang.Runnable> arrayBlockingQueue = new ArrayBlockingQueue<java.lang.Runnable>(100);
        // Semaphore semaphore = new Semaphore(50);
        try {
            Configuration conf = HdfsCUtils.getCfg();
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("hdev@YYDEVOPS.COM", "/home/liangrui/hdev.keytab");
            DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
            if (fs.exists(wirteFile) || fs.exists(not_error_wirteFile)) {
                System.out.println("file exists exit===" + wirteFile);
                System.exit(0);
            }
            //reader file path list
            Path path = new Path(filePath);
            FSDataInputStream inputStream = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            //存储线程的返回值
            List<String> results = new ArrayList<>();
            int i = 0;
            String line = reader.readLine();
            while (line != null) {
                i++;
                line = reader.readLine();
                if (StringUtils.isBlank(line)) {
                    continue;
                }
                results.add(line);
                if (results.size() >= 5000) {// limit 100[
                    List<String> limitList = results.stream().map(e -> e).collect(Collectors.toList());
                    RunTabThread2 task = new RunTabThread2(fs, limitList);
                    // Future<String> future = executorService.submit(task);
                    executorService.submit(task);
                    results.clear();
                }

            }
            RunTabThread2 task = new RunTabThread2(fs, results);
            executorService.submit(task);
            reader.close();

            //CountDownLatch latch = new CountDownLatch(results.size());
            //     for (String read_line : results) {
//                RunTabThread2 task = new RunTabThread2(fs, read_line.trim(),latch);
//                // Future<String> future = executorService.submit(task);
//                executorService.submit(task);
//                System.out.println("i=" + i+" get count"+latch.getCount() );
            //  }
            //get restult
            //write error
            //latch.wait();
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
                FSDataOutputStream outputStream = fs.create(wirteFile, true);
                outputStream.writeBytes(sb.toString());
                outputStream.close();
                System.out.println("write sb ok===" + sb.toString());

            } else {
                FSDataOutputStream outputStream = fs.create(not_error_wirteFile, true);
                outputStream.writeBytes("succeed");
                outputStream.close();
                System.out.println("filePath not error======" + filePath);
            }
        } catch (Exception e) {

            e.printStackTrace();
        }
        String endTime = new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date());
        System.out.println(endTime + "=======all end==============");
    }

    public static class RunTabThread2 implements Runnable {
        //        private final String line;
        private final DistributedFileSystem dfs;
        private CountDownLatch countDownLatch;
        private List<String> limitList;


        public RunTabThread2(DistributedFileSystem dfs, List<String> limitList) {
            this.dfs = dfs;
            this.limitList = limitList;
        }

        public void run() {
            String ret = null;
            try {
                //semaphore.acquire();
                for (String line : limitList) {
                    EcUtils eu = new EcUtils();
                    Map<String, String> result = eu.checkEC("/hive_warehouse/" + line, dfs);
                    //System.out.println("hello+++" + line);
                    if ("-1".equals(result.get("status"))) {
                        ret = line + "===" + result.get("msg");
                        System.out.println(ret);
                        sb.append(ret).append("\n");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                countDownLatch.countDown();
                System.gc();
            }
        }


    }
}
