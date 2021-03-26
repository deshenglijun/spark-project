package com.desheng.bigdata.common.db;

import com.desheng.bigdata.common.CommonUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.LinkedList;

public class HBaseConnectionPool {
    //池子
    private static LinkedList<Connection> pool = new LinkedList<Connection>();
    //初始化
    static {
        try {
            Configuration configuration = HBaseConfiguration.create();
//            configuration.set("hbase.rootdir", "hdfs://bigdata01:9000/hbase");
//            configuration.set("hbase.cluster.distributed", "true");
//            configuration.set("hbase.zookeeper.quorum", "bigdata01,bigdata02,bigdata03");
            for (int i = 0; i < 5; i++) {
                Connection connection = ConnectionFactory.createConnection(configuration);
                pool.push(connection);
            }
        }catch (Exception e) {
            throw new ExceptionInInitializerError();
        }
    }

    public static Connection getConnection() {
        while(pool.isEmpty()) {
            CommonUtil.sleep(1000);
        }
        return pool.poll();
    }

    public static void release(Connection connection) {
        pool.push(connection);
    }

    public static Table getTable(Connection connection, String tableName) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    public static Table getTable(Connection connection, TableName tableName) {
        Table table = null;
        try {
            table = connection.getTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;

    }

    public static void main(String[] args) throws IOException {
        Connection connection = getConnection();
        TableName[] tableNames = connection.getAdmin().listTableNames();
        for(TableName tblName : tableNames) {
            System.out.println(tblName.toString());
        }
        release(connection);
    }
}
