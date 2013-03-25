package org.apache.ivory.cluster.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class ImpersonationTest {

    public static void main(String[] args)
            throws IOException, InterruptedException {
        oozieImpersonatingSeetharam();
        seetharamImpersonatingSeetharam();
    }

    private static void oozieImpersonatingSeetharam()
            throws IOException, InterruptedException {
        UserGroupInformation proxy =
                UserGroupInformation.createProxyUser("oozie",
                UserGroupInformation.createRemoteUser("seetharam"));

        FileSystem fs = proxy.doAs(new PrivilegedExceptionAction<FileSystem>() {
            @Override
            public FileSystem run() throws Exception {
                Configuration conf = new Configuration();
                conf.set("fs.default.name", "hdfs://127.0.0.1:54310");
                return FileSystem.get(conf);
            }
        });

        System.out.println("ImpersonationTest.oozieImpersonatingSeetharam");
        System.out.println(fs.listStatus(new Path("/")).length);
    }

    private static void seetharamImpersonatingSeetharam()
            throws IOException, InterruptedException {
        UserGroupInformation proxy =
                UserGroupInformation.createProxyUser("seetharam",
                UserGroupInformation.createRemoteUser("seetharam"));

        FileSystem fs = proxy.doAs(new PrivilegedExceptionAction<FileSystem>() {
            @Override
            public FileSystem run() throws Exception {
                Configuration conf = new Configuration();
                conf.set("fs.default.name", "hdfs://127.0.0.1:54310");
                return FileSystem.get(conf);
            }
        });

        System.out.println("ImpersonationTest.seetharamImpersonatingSeetharam");
        System.out.println(fs.listStatus(new Path("/")).length);
    }
}
