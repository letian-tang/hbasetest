package com.zhoupu.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTest2 {

    public static void main(String[] args) {
        /**
         * 当你调用一个静态的create()方法时，代码会尝试使用当前的
         * java classpath来载入两个配置文件hbase-default.xml和hbase-site.xml
         * 如果使用create(Configuration that)方法指定一个已存在的配置，那么与所有从classpath载入的配置想比，用户
         * 指定的配置优先级最高。
         */      
        Configuration conf = HBaseConfiguration.create();
        
        /**
         * 用户提交的可选配置，在使用table实例之前，用户可以任意的修改配置。如下
         * 也就可以简单的忽略任何外部的客户端配置文件，而直接在代码中设置属性，这样
         * 就创建了一个不需要额外配置的客户端。
         */
        //conf.set("hbase.zookeeper.quorum", "zk1.foo.com,zk2.foo.com");
        try {
            Connection connection = ConnectionFactory.createConnection(conf);

            HTable hTable = (HTable) connection.getTable(TableName.valueOf("table"));
            
            //设置自动刷新为false，启用客户端写缓冲区
            hTable.setAutoFlushTo(false);
            
            //如果没有配置，可以手工指定缓冲区大小
            hTable.setWriteBufferSize(20971520);
            

            Put put = new Put(Bytes.toBytes("rk0001"));
            
            hTable.checkAndPut(Bytes.toBytes("rk0001"), Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), null, put);

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));

            hTable.put(put);

            hTable.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
