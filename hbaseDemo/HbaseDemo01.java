package com.datatist.hbaseDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//操作数据库  第一步：获取连接  第二步：获取客户端对象   第三步：操作数据库  第四步：关闭
public class HbaseDemo01 {
    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","node01:2181,node02:2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        //获取管理员对象，来对数据库进行DDL得操作
        Admin admin = conn.getAdmin();
        //指定我们得表名
        TableName table = TableName.valueOf("myuser");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
        //指定列族
        HColumnDescriptor f1 = new HColumnDescriptor("f1");
        HColumnDescriptor f2 = new HColumnDescriptor("f2");
        hTableDescriptor.addFamily(f1);
        hTableDescriptor.addFamily(f2);
        admin.createTable(hTableDescriptor);
        admin.close();
        conn.close();

    }

    private Connection connection;
    private final String TABLE_NAME = "myuser";
    private Table table;

    @After
    public void close(){

    }

    @Before
    public void init() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","node01:2181,node02:2181");
        connection = ConnectionFactory.createConnection(conf);
        table = connection.getTable(TableName.valueOf(TABLE_NAME));
    }

    //向表中添加数据
    @Test
    public void addData() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        //创建put对象，并指定rowkey
        Put put = new Put("0001".getBytes());
        put.addColumn("f1".getBytes(),"name".getBytes(),"zhangsan".getBytes());
        put.addColumn("f1".getBytes(),"age".getBytes(), Bytes.toBytes(18));
        put.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(25));
        put.addColumn("f1".getBytes(),"address".getBytes(),Bytes.toBytes("地球人"));

        table.put(put);
        table.close();
    }

    //插入测试数据
    @Test
    public void insertBatchData() throws IOException {

        //获取连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取表
        Table myuser = connection.getTable(TableName.valueOf("myuser"));
        //创建put对象，并指定rowkey
        Put put = new Put("0002".getBytes());
        put.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(1));
        put.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("曹操"));
        put.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(30));
        put.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("沛国谯县"));
        put.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("16888888888"));
        put.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("helloworld"));

        Put put2 = new Put("0003".getBytes());
        put2.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(2));
        put2.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("刘备"));
        put2.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(32));
        put2.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put2.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("幽州涿郡涿县"));
        put2.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("17888888888"));
        put2.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("talk is cheap , show me the code"));


        Put put3 = new Put("0004".getBytes());
        put3.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(3));
        put3.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("孙权"));
        put3.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(35));
        put3.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put3.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("下邳"));
        put3.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("12888888888"));
        put3.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("what are you 弄啥嘞！"));

        Put put4 = new Put("0005".getBytes());
        put4.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(4));
        put4.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("诸葛亮"));
        put4.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(28));
        put4.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put4.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("四川隆中"));
        put4.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("14888888888"));
        put4.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("出师表你背了嘛"));

        Put put5 = new Put("0006".getBytes());
        put5.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(5));
        put5.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("司马懿"));
        put5.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(27));
        put5.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put5.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("哪里人有待考究"));
        put5.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("15888888888"));
        put5.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("跟诸葛亮死掐"));


        Put put6 = new Put("0007".getBytes());
        put6.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(5));
        put6.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("xiaobubu—吕布"));
        put6.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(28));
        put6.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put6.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("内蒙人"));
        put6.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("15788888888"));
        put6.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("貂蝉去哪了"));

        List<Put> listPut = new ArrayList<Put>();
        listPut.add(put);
        listPut.add(put2);
        listPut.add(put3);
        listPut.add(put4);
        listPut.add(put5);
        listPut.add(put6);

        myuser.put(listPut);
        myuser.close();
        connection.close();
    }


    /**
     * 查询rowkey为0003的人
     */
    @Test
    public void getData() throws IOException{
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

        Get get = new Get(Bytes.toBytes("0003"));
        get.addFamily("f1".getBytes()); //限制只查询f1列族下面所有列的值
        get.addColumn("f2".getBytes(),"phone".getBytes());//查询f2列族得phone这个字段 Result result = table.get(get);
        Result result = table.get(get);//通过get查询，返回一个result对象，所有的字段的数据都是封装在result里面了
        //获取一条数据所有的cell，所有数据值都是在cell里面的
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            byte[] familyName = CellUtil.cloneFamily(cell);//列族名
            byte[] columnName = CellUtil.cloneQualifier(cell);//列名
            byte[] rowkey = CellUtil.cloneRow(cell);//rowkey
            byte[] cellValue = CellUtil.cloneValue(cell);//cell值
            //注意这里要判断字段的数据类型，当age,id列时，cell值是int类型，
            // 使用对应的转换的方法，才能够获取到值
            if ("age".equals(Bytes.toString(columnName)) || "id".equals(Bytes.toString(columnName))){
                System.out.println(Bytes.toString(familyName));
                System.out.println(Bytes.toString(columnName));
                System.out.println(Bytes.toString(rowkey));
                System.out.println(Bytes.toInt(cellValue));
            }else{
                System.out.println(Bytes.toString(familyName));
                System.out.println(Bytes.toString(columnName));
                System.out.println(Bytes.toString(rowkey));
                System.out.println(Bytes.toString(cellValue));
            }
        }
        table.close();
    }
}
