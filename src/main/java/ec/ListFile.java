package ec;

import com.yy.bigdata.utils.HdfsCUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 指定hdfs目录，统计目录下所有文件，并写入hdfs /user/hdev/ec_file/{table}
 * <p>
 * 例：
 * nohup hadoop jar hdfs-client-op-1.0-SNAPSHOT.jar  ec.ListFile /hive_warehouse  > run_alltable.log 2>&1 &
 * hadoop jar hdfs-client-op-1.0-SNAPSHOT.jar  ec.ListFile /hive_warehouse_repl
 */
public class ListFile {


    public static void main(String[] args) {
        String[] skipdb = new String[]{"/hive_warehouse/bigo_export.db",
                "/hive_warehouse/biugolite.db",
                "/hive_warehouse/error_back",
                "/hive_warehouse/error_back2",
                "/hive_warehouse/text_error_back",
                "/hive_warehouse/hdfs-archive",
                "/hive_warehouse/hive_warehouse_repl",
                "/hive_warehouse/inbilin_db_xunni_user_grouplike_record_test.har",
                "/hive_warehouse/onepiece.db",
                "/hive_warehouse/project_stream_pwc.db",
                "/hive_warehouse/projectstream.db",
                "/hive_warehouse/recover",
                "/hive_warehouse/rs_ec_client.db",
                "/hive_warehouse/test",
                "/hive_warehouse/warehouse_old_snapshots",
                "/hive_warehouse/yy_recom.db",
                "/hive_warehouse/dw_oversea_pub.db",
                "/hive_warehouse/ec_check_list"
        };

        String table_dir = "";
        System.out.println("main args " + Arrays.toString(args));
        if (args.length >= 1) {
            table_dir = args[0];
        }
        ExecutorService executor = Executors.newFixedThreadPool(5);
        //CountDownLatch countDownLatch = new CountDownLatch(10);
        try {
            Configuration conf = HdfsCUtils.getCfg();
            DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("hdev@YYDEVOPS.COM", "/home/liangrui/hdev.keytab");
            List<String> allDBDir = getSubDirectory(fs, table_dir, skipdb);
            //for db
            for (String db : allDBDir) {
                //get table
                List<String> allTaable = getSubDirectory(fs, db, null);//this db get all table
                for (String tableDir : allTaable) {
                    if ("/hive_warehouse/hiidosdk.db/yy_mbsdkevent_hour_original".equals(tableDir)) {
                        System.out.println("skip thread===" + tableDir);
                        continue;
                    }
                    Runnable tableRun = new RunTabThread(conf, tableDir);
                    //Thread tableThread = new Thread(tableRun);
                    //tableThread.setDaemon(true);
                    executor.submit(tableRun);
                    System.out.println("Starting tableThread thread===" + tableDir);
                }
            }
            //merger file
            executor.shutdown();
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public static List<String> getSubDirectory(DistributedFileSystem fs, String table_dir, String[] skipdb) throws IOException {
        List<String> allDBDir = new ArrayList<String>();
        RemoteIterator<LocatedFileStatus> dirStatus = fs.listLocatedStatus(new Path(table_dir)); //list all dir
        while (dirStatus.hasNext()) {
            LocatedFileStatus lfs = dirStatus.next();
            String db_dir = lfs.getPath().toUri().getPath().replace("hdfs://yycluster06", "");
            if (skipdb != null && skipdb.length > 0) {
                if (ArrayUtils.contains(skipdb, db_dir)) {
                    System.out.println("skip db==" + db_dir);
                    continue;
                }
            }
            allDBDir.add(db_dir);
            // System.out.println(db_dir);
        }
        return allDBDir;
    }

    public static void wirteTableFile(Configuration conf, String table_dir) throws IOException, InterruptedException {
        DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("hdev@YYDEVOPS.COM", "/home/liangrui/hdev.keytab");
        RemoteIterator<LocatedFileStatus> rlfss = fs.listFiles(new Path(table_dir), true);
        List<String> allFileList = new ArrayList<String>();
        while (rlfss.hasNext()) {
            LocatedFileStatus lfs = rlfss.next();
            allFileList.add(lfs.getPath().toUri().getPath());
        }
        if (allFileList.size() > 0) {
            StringBuilder text = new StringBuilder();
            for (String lien : allFileList) {
                text.append(lien.replace("/hive_warehouse/", "").replace("/hive_warehouse_repl/", "")).append("\n");
            }
            ///hive_warehouse/hiidosdk.db/yy_mbsdkevent_hour_original
            String saveName = table_dir.replace("/hive_warehouse/", "").replace(".db/", "_db_");
            //int slash = table_dir.lastIndexOf("/");
            //String saveName = table_dir.substring(slash + 1);
            Path wirteFile = new Path("/user/hdev/ec_file/" + saveName + ".txt");
            FSDataOutputStream outputStream = fs.create(wirteFile, true);
            outputStream.writeBytes(text.toString());
            Thread.sleep(10000);
            outputStream.close();
        }
        fs.close();
    }


    public static class RunTabThread implements Runnable {
        private String tableDir;
        private Configuration conf;

        public RunTabThread(Configuration conf, String tableDir) {
            this.tableDir = tableDir;
            this.conf = conf;
        }

        public void run() {
            System.out.println("run=" + tableDir);
            try {

                wirteTableFile(conf, tableDir);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
