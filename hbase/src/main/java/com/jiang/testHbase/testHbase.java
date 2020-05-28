package com.jiang.testHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class testHbase {

    private static Admin admin = null;
    private static Connection conn = null;
    private static Configuration conf = null;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.1.102");
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void close() throws IOException {
        if (admin != null) {
            admin.close();
        }
        if (conn != null) {
            conn.close();
        }
    }


    //table isExist
    public static boolean tableIsExists(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    //create table
    public static void createTable(String tableName, String... cfs) throws IOException {
        if (tableIsExists(tableName)) {
            System.out.println("table exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : cfs) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
            System.out.println("table created");
        }

    }

    //drop table
    private static void dropTable(String tableName) throws IOException {
        if (!tableIsExists(tableName)) {
            return;
        }
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println("table dropped");
    }

    // add update
    public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {
        Table hTable = conn.getTable(TableName.valueOf(tableName));
        //List<Put> putList;
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("sex"), Bytes.toBytes("male"));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("age"), Bytes.toBytes(18));
        hTable.put(put);
        hTable.close();
    }

    //delete
    public static void delete(String tableName, String rowKey, String cf, String cn) throws IOException {
        Table hTable = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
//      delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));
        hTable.delete(delete);
        hTable.close();
    }

    //search
    public static void scanTable(String tableName) throws IOException {
        Table hTable = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = hTable.getScanner(scan);

        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("VAL:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        hTable.close();
    }

    public static void getTable(String tableName, String rowKey, String cf, String cn) throws IOException {
        Table hTable = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        //get.setMaxVersions();
        Result result = hTable.get(get);

        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("VAL:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        hTable.close();
    }

    public static void main(String[] args) throws IOException {
        createTable("master");
        putData("master", "1001", "info", "name", "jiang");
        putData("master", "1003", "info", "name", "xiao");
        scanTable("master");
        delete("master", "1001", "info", "name");
        getTable("master", "1001", "info", "name");
        dropTable("master");
        close();
    }
}
