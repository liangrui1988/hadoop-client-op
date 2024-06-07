package com.yy.bigdata.orc;

import com.yy.bigdata.utils.HdfsCUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.io.IOException;

public class OrcUtils {

    /**
     * read orc file check,if orc failure throws IOException
     *
     * @param filePath
     * @return boolean (ture=file normal)  ()
     * @throws IOException
     */
    public static boolean readOrcCheck(String filePath) {
        boolean is_normal = true;
        RecordReader rows = null;
        try {
            Configuration conf = HdfsCUtils.getCfg();
            DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("hdev@YYDEVOPS.COM", "/home/liangrui/hdev.keytab");
            Reader reader = OrcFile.createReader(new Path(filePath), new OrcFile.ReaderOptions(conf).filesystem(fs));
            //OrcFile.createReader(new Path(filePath),OrcFile.readerOptions(conf));
            System.out.println(reader.getMetadataKeys());
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
        } catch (IOException ioe) {
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
}
