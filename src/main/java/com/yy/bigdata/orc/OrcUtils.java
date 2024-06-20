package com.yy.bigdata.orc;

import com.yy.bigdata.utils.HdfsCUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class OrcUtils {

    /**
     * read orc file check,if orc failure throws IOException
     *
     * @param filePath
     * @return boolean (ture=file normal)  ()
     * @throws IOException
     */
    public static boolean readOrcCheck(String filePath, String skipIp) {
        boolean is_normal = true;
        RecordReader rows = null;
        try {
            Configuration conf = HdfsCUtils.getCfg();
            conf.set("ext.skip.ip", skipIp);
            DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("hdev@YYDEVOPS.COM", "/home/liangrui/hdev.keytab");
            Reader reader = OrcFile.createReader(new Path(filePath), new OrcFile.ReaderOptions(conf).filesystem(fs));
            //OrcFile.createReader(new Path(filePath),OrcFile.readerOptions(conf));
            //System.out.println(reader.getMetadataKeys());
            //System.out.println(reader.getRawDataSize());
            System.out.println(reader.getNumberOfRows());
            System.out.println(reader.getFileVersion());
            System.out.println(reader.getSchema());
            //System.out.println(reader.getContentLength());
            //System.out.println(reader.getRawDataSize());
            System.out.println("print rows================");
            rows = reader.rows();
            System.out.println(rows.getRowNumber());
            System.out.println(rows);
            VectorizedRowBatch batch = reader.getSchema().createRowBatch();
            long i = 0; // read data check is error
            while (rows.nextBatch(batch)) {
                i=batch.size;
            }
            System.out.println("last batch size=="+i);
        } catch (Exception ioe) {
            is_normal = false;
            ioe.printStackTrace();
        } finally {
            if (rows != null) {
                try {
                    rows.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();

                }
            }
        }
        return is_normal;
    }

    public static List<String> readLine(String file_path) {
        List<String> allLines = null;
        try {
            java.nio.file.Path path = Paths.get(file_path);
            byte[] bytes = Files.readAllBytes(path);
            allLines = Files.readAllLines(path, StandardCharsets.UTF_8);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return allLines;
    }

}
