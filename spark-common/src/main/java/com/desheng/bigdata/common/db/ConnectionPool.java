package com.desheng.bigdata.common.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

/**
 * 自定义的数据库连接池
 */
public class ConnectionPool {

    //池子
    private static LinkedList<Connection> pool = new LinkedList<>();
    //初始化
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://localhost:3306/test";
            String user = "mark";
            String password = "sorry";
            for (int i = 0; i < 5; i++) {
                Connection connection = DriverManager.getConnection(url, user, password);
                pool.push(connection);
            }
        }catch (Exception e) {
            throw new ExceptionInInitializerError();
        }
    }

    public static Connection getConnection() {
        while(pool.isEmpty()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return pool.poll();
    }

    public static void release(Connection connection) {
        pool.push(connection);
    }
}
