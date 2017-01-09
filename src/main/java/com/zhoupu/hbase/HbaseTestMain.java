package com.zhoupu.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author tangdingyi
 *
 */
public class HbaseTestMain {


    private Configuration configuration;

    @Before
    public void init() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "60010");


        /**
         * 这里hbase.zookeeper.quorum的属性与hbase-site.xml中相对应的设置有关
         * 1.在hbase-site.xml中，设置hbase.zookeeper.quorum为本地地址时，在填写以下hbase.zookeeper.quorum参数值时，请加上端口。
         * ex:
         * hbase-site.xml:
         * <property>
         *      <name>hbase.zookeeper.quorum</name>
         *      <value>172.16.43.10</value>
         *  </property>
         * configuration.set("hbase.zookeeper.quorum","172.16.43.10:2181");
         * 注：2181端口是根据zookeeper中zoo.cfg设置的clientPort=2181值，也可通过登录Hbase Master管理页面，在最底下可以看到
         * zookeeper的信息，可以直接把那边的拷贝过来。
         * 2.在hbase-site.xml中，填写的是几个zookeeper地址，则将其以分号为分隔填入
         * ex:
         * hbase-site.xml:
         * <property>
         *      <name>hbase.zookeeper.quorum</name>
         *      <value>172.16.43.10:2181,172.16.43.10:2182,172.16.43.10:2183</value>
         *  </property>
         * configuration.set("hbase.zookeeper.quorum","172.16.43.10:2181,172.16.43.10:2182,172.16.43.10:2183");
         */
        configuration.set("hbase.zookeeper.quorum",
                "master:2181,node1:2181,node2:2181");
        configuration.set("hbase.master", "master:60010");
    }


    /**
     * 创建表
     * 
     * @throws Exception
     */
    @Test
    public void createTable() {
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(configuration);
            HBaseAdmin hBaseAdmin = (HBaseAdmin) conn.getAdmin();
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("test_zp"));
            // 添加列簇
            desc.addFamily(new HColumnDescriptor("test_1"));
            desc.addFamily(new HColumnDescriptor("test_2"));
            desc.addFamily(new HColumnDescriptor("test_3"));
            if (hBaseAdmin.tableExists("test_zp")) {
                System.out.println("table is exists !");
                System.exit(0);
            } else {
                hBaseAdmin.createTable(desc);
                System.out.println("成功创建表！");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != conn) {
                try {
                    conn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


    }
    
    
    /**
     * 插入数据
     * @throws Exception 
     */
    @Test
    public void insertData(){
        Connection conn = null;
        HTable hTable = null;
        try{
            conn = ConnectionFactory.createConnection(configuration);
            hTable = (HTable) conn.getTable(TableName.valueOf("test_zp"));
            //一个PUT代表一行，构造函数传入的是RowKey
            Put put = new Put((String.valueOf(System.currentTimeMillis())).getBytes());
            put.addColumn("test_1".getBytes(), null, "这是第一行第一列的数据".getBytes());
            put.addColumn("test_2".getBytes(), null, "这是第一行第二列的数据".getBytes());
            put.addColumn("test_3".getBytes(), null, "这是第一行第三列的数据".getBytes());
            
            //增加一行
            Put put2 = new Put((String.valueOf(System.currentTimeMillis())).getBytes());
            put2.addColumn("test_1".getBytes(), null, "这是第二行第一列的数据".getBytes());
            put2.addColumn("test_2".getBytes(), null, "这是第二行第二列的数据".getBytes());
            put2.addColumn("test_3".getBytes(), null, "这是第二行第三列的数据".getBytes());
            
           //增加一行
            Put put3 = new Put((String.valueOf(System.currentTimeMillis())).getBytes());
            put3.addColumn("test_1".getBytes(), Bytes.toBytes("name1"), "这是第三行第一列的数据".getBytes());
            put3.addColumn("test_2".getBytes(), Bytes.toBytes("name2"), "这是第三行第二列的数据".getBytes());
            put3.addColumn("test_3".getBytes(), Bytes.toBytes("name3"), "这是第三行第三列的数据".getBytes());
            
            List<Put> puts = new ArrayList<Put>();
            puts.add(put);
            puts.add(put2);
            puts.add(put3);
            //添加进表中
            hTable.put(puts);
            
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            if(null != hTable){
                try {
                    hTable.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(null != conn){
                try {
                    conn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        
    }
    
    
    
    /**
     * 查询时，会每个cell代表一个列簇中的一个区域，
     * 例如：有一个列簇为 test_1
     * 1.如果存储数据时没有存储列修饰符，则cell代表整个列簇的内容，查询出的就是该行下整个列簇的内容
     * 2.如果存储数据时有存储列修饰符，则每个列簇下的列修饰符各有一个cell
     * 
     */
    @Test
    public void query(){
        
        Connection conn = null;
        HTable table = null;
        ResultScanner scann = null;
        try{
            conn = ConnectionFactory.createConnection(configuration);
            table = (HTable) conn.getTable(TableName.valueOf("test_zp"));
            
            scann = table.getScanner(new Scan());
            /**
             * 循环读取按行区分：
             * 读取结果为：
             *  该表RowKey为：1445320222118
                                    列簇为：test_1
                                    值为：这是第一行第一列的数据
                                    列簇为：test_2
                                    值为：这是第一行第二列的数据
                                    列簇为：test_3
                                    值为：这是第一行第三列的数据
                                    ==========================================
                                    该表RowKey为：1445320222120
                                    列簇为：test_1
                                    值为：这是第二行第一列的数据
                                    列簇为：test_2
                                    值为：这是第二行第二列的数据
                                    列簇为：test_3
                                    值为：这是第二行第三列的数据
                ==========================================
                
             */
            for (Result rs : scann) {
                System.out.println("该表RowKey为：" + new String(rs.getRow()));
                /**
                 * 这边循环是按cell进行循环
                 */
                for (Cell cell : rs.rawCells()) {
                    System.out.println("列簇为：" + new String(CellUtil.cloneFamily(cell)));
                    System.out.println("列修饰符为：" + new String(CellUtil.cloneQualifier(cell)));
                    System.out.println("值为：" + new String(CellUtil.cloneValue(cell)));
                }
                System.out.println("==========================================");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != scann) {
                scann.close();
            }
            if (null != table) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != conn) {
                try {
                    conn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        
    }
    
    
    /**
     * 根据RowKey查询单行
     */
    @Test
    public void queryByRowKey(){
        Connection conn = null;
        HTable table = null;
        try{
            conn = ConnectionFactory.createConnection(configuration);
            table = (HTable) conn.getTable(TableName.valueOf("test_zp"));
            Get get = new Get("1483949313700".getBytes());
            
            Result rs = table.get(get);
            System.out.println("test表 RowKey为1483949313700的行数据如下：");
            for(Cell cell : rs.rawCells()){
                //疑问：同个行，一个列簇里具有多列的查询？
                System.out.println("列簇为："+new String(CellUtil.cloneFamily(cell)));
                System.out.println("值为："+new String(CellUtil.cloneValue(cell)));
            }
        }catch(Exception e ){
            e.printStackTrace();
        }finally{
            
            if(null != table){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(null != conn){
                try {
                    conn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    
   
    
    
    /**
     * 向一个列簇中插入多个值
     */
    @Test
    public void insertColumsValue(){
        Connection conn = null;
        HTable table = null;
        try{
            conn = ConnectionFactory.createConnection(configuration);
            table = (HTable) conn.getTable(TableName.valueOf("test_zp"));
            Put put = new Put("1483949313700".getBytes());
            //1.如果没有指定列修饰符，而在这之下已经有内容，则覆盖原先内容
            //2.如果有指定列修饰符，而在该列修饰符下如果存在内容则覆盖
            put.addColumn("test_1".getBytes(), null,"这是第一行第一列的第二个数值".getBytes());
            table.put(put);
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            if(null != table){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            
            if(null != conn){
                try {
                    conn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        
    }
    
    /**
     * 添加数据时，添加列修饰符
     * 列修饰符：相当于在一个列簇中，根据列修饰符分隔成不同区域存储内容。（HBase的特性）
     * 插入后，查询到的数值：
     * 该表RowKey为：1445320222118
                列簇为：test_1
                列修饰符为：
                值为：这是第一行第一列的第二个数值
                列簇为：test_1
                列修饰符为：1
                值为：test_1_1
                列簇为：test_1
                列修饰符为：2
                值为：test_1_2
                列簇为：test_2
                列修饰符为：
                值为：这是第一行第二列的数据
                列簇为：test_3
                列修饰符为：
                值为：这是第一行第三列的数据
                ==========================================
                该表RowKey为：1445320222120
                列簇为：test_1
                列修饰符为：
                值为：这是第二行第一列的数据
                列簇为：test_2
                列修饰符为：
                值为：这是第二行第二列的数据
                列簇为：test_3
                列修饰符为：
                值为：这是第二行第三列的数据
                ==========================================
     */
    @Test
    public void insertrAddColumnQualifier(){
        
        Connection conn = null;
        HTable table = null;
        try{
            conn = ConnectionFactory.createConnection(configuration);
            table = (HTable) conn.getTable(TableName.valueOf("test_zp"));
            Put put = new Put("1483949313700".getBytes());
            put.addColumn("test_1".getBytes(), "1".getBytes(), "test_1_1".getBytes());
            put.addColumn("test_1".getBytes(), "2".getBytes(), "test_1_2".getBytes());
            
            table.put(put);
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try{
                table.close();
                conn.close();
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        
    }
    


    /**
     * 删除指定名称的列簇
     */
    @Test
    public void deleteFamily(){
        
        Connection conn = null;
        HBaseAdmin admin = null;
        try{
            conn = ConnectionFactory.createConnection(configuration);
            admin = (HBaseAdmin) conn.getAdmin();
            admin.deleteColumn("test_zp".getBytes(), "test_3");
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try{
                if(null != conn){
                    conn.close();
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }
    
    
    
    /**
     * 删除指定行
     */
    @Test
    public void deleteRow(){
        
        Connection conn = null;
        HTable table = null;
        try{
            conn = ConnectionFactory.createConnection(configuration);
            table = (HTable) conn.getTable(TableName.valueOf("test_zp"));
            table.delete(new Delete("1483949313700".getBytes()));
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try{
                if(null != table){
                    table.close();
                }
                if(null != conn){
                    conn.close();
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        
    }
    
    
    
    
    /**
     * 删除指定表名
     */
    @Test
    public void deleteTable(){
        
        Connection conn = null;
        HBaseAdmin admin = null;
        try{
            conn = ConnectionFactory.createConnection(configuration);
            admin = (HBaseAdmin) conn.getAdmin();
            //在删除一张表前，要使其失效
            admin.disableTable("test_zp");
            admin.deleteTable("test_zp");
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try{
                if(null != conn){
                    conn.close();
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        
    }
    



}
