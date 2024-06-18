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
 * 单张表统计出表的所有文件
 *
 * nohup hadoop jar hdfs-client-op-1.0-SNAPSHOT.jar  ec.ListFileSigleTab /hive_warehouse/hiidosdk.db/yy_mbsdkevent_hour_original & > run_sdk_yy_mbsdkevent_hour_original.log
 */
public class ListFileSigleTab {


    public static void main(String[] args) {


        String table_dir = "";
        System.out.println("main args " + Arrays.toString(args));
        if (args.length >= 1) {
            table_dir = args[0];
        } else {
            System.exit(0);
        }

        try {
            Configuration conf = HdfsCUtils.getCfg();
            //UserGroupInformation.setConfiguration(conf);
            //UserGroupInformation.loginUserFromKeytab("hdev@YYDEVOPS.COM", "/home/liangrui/hdev.keytab");
            wirteTableFile(conf, table_dir);
        } catch (Exception e) {
            e.printStackTrace();
        }

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
            //System.out.println(text.toString().length());
            int slash = table_dir.lastIndexOf("/");
            String saveName = table_dir.substring(slash + 1);
            Path wirteFile = new Path("/user/hdev/sigle_ec_file/" + saveName + ".txt");
            FSDataOutputStream outputStream = fs.create(wirteFile, true);
            outputStream.writeBytes(text.toString());
            outputStream.close();
        }
        fs.close();
    }


}
