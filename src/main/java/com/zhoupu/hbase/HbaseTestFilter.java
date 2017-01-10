package com.zhoupu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HbaseTestFilter {
    
    
    /**
     * 
     * @throws Exception
     */
    private HTable getHtable() throws Exception {
        
        Configuration conf = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(conf);

        HTable hTable = (HTable) connection.getTable(TableName.valueOf("table"));
        
        return hTable;
    }



    /**
     * 行键过滤器
     * RowFilter
     */
    @Test
    public void testRowFilter() throws Exception {


        HTable hTable = getHtable();

        Scan scan = new Scan();
        scan.addColumn("colfam1".getBytes(), Bytes.toBytes("col-1"));

        /**
         * 指定比较运算符和比较器，这里需要精确匹配。等于或小于rowkey配置的给定行
         */
        Filter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes("row-22")));

        scan.setFilter(rowFilter);

        ResultScanner scanner = hTable.getScanner(scan);

        for (Result rs : scanner) {
            System.out.println(rs);
        }

        scanner.close();
        
        /**
         * 使用正则表达式来匹配行键
         */
        Filter rowFilter2 = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator(".*-.5"));

        scan.setFilter(rowFilter2);

        ResultScanner scanner2 = hTable.getScanner(scan);

        for (Result rs : scanner2) {
            System.out.println(rs);
        }

        scanner2.close();
        
        /**
         * 使用子串匹配
         */
        Filter rowFilter3 = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("-5"));

        scan.setFilter(rowFilter3);

        ResultScanner scanner3 = hTable.getScanner(scan);

        for (Result rs : scanner3) {
            System.out.println(rs);
        }

        scanner3.close();


    }
    
    
    
    
    /**
     * 列族过滤器
     * @throws Exception
     */
    @Test
    public void testFamilyFilter() throws Exception {

        HTable hTable = getHtable();

        Scan scan = new Scan();
        scan.addColumn("colfam1".getBytes(), Bytes.toBytes("col-1"));

        /**
         * 指定比较运算符和比较器，这里需要精确匹配。等于或小于rowkey配置的给定行
         */
        Filter failter1 = new FamilyFilter(CompareFilter.CompareOp.LESS,
                new BinaryComparator(Bytes.toBytes("colfam3")));

        scan.setFilter(failter1);

        ResultScanner scanner = hTable.getScanner(scan);

        for (Result rs : scanner) {
            System.out.println(rs);
        }

        scanner.close();
        
        /**
         * 使用正则表达式来匹配行键
         */
        Get get1 = new Get(Bytes.toBytes("row-5"));
        get1.setFilter(failter1);
        Result result1 = hTable.get(get1);
        System.out.println("Result of get():"+result1);
        
        
        Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes("colfam3")));

        Get get2 = new Get(Bytes.toBytes("row-5"));
        get2.addFamily(Bytes.toBytes("colfam1"));
        get2.setFilter(filter2);

        Result result2 = hTable.get(get1);
        System.out.println("Result of get():"+result2);
        
    }
    
    
    /**
     * 列名过滤器
     * @throws Exception
     */
    @Test
    public void testQualifierFilter() throws Exception {
        
        HTable hTable = getHtable();
        Scan scan = new Scan();
        
        Filter failter = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes("col-2")));
        
        scan.setFilter(failter);
        
        ResultScanner scanner = hTable.getScanner(scan);

        for (Result rs : scanner) {
            System.out.println(rs);
        }

        scanner.close();
        
        Get get = new Get(Bytes.toBytes("row-5"));
        get.setFilter(failter);
        Result result = hTable.get(get);
        System.out.println("Result of get():"+result);
    }
    
    
    /**
     * 值过滤器
     * @throws Exception
     */
    @Test
    public void testValueFilter() throws Exception {
        
        HTable hTable = getHtable();
        Scan scan = new Scan();
        
        Filter failter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("0.4"));
        
        scan.setFilter(failter);
        
        ResultScanner scanner = hTable.getScanner(scan);

        for (Result rs : scanner) {
            System.out.println(rs);
        }

        scanner.close();
        
        Get get = new Get(Bytes.toBytes("row-5"));
        get.setFilter(failter);
        Result result = hTable.get(get);
        System.out.println("Result of get():"+result);
    }
    
    
    /**
     * 参考过滤器
     * @throws Exception
     */
    

}
