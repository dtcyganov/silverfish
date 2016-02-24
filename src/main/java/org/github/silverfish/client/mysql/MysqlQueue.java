package org.github.silverfish.client.mysql;

import org.github.silverfish.client.Queue;

import java.sql.*;

public class MysqlQueue<E> implements Queue<E> {

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //TODO: remove
    private void test() throws Exception {
        // some interaction example

        String url = "jdbc:mysql://host:port/scheme";
        String password = "";
        String user = "";

        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            try (PreparedStatement stmt = connection.prepareStatement("select 1")) {
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        rs.next();
                    }
                }
            }
        }
    }

    @Override
    public void enqueue(E e) {

    }

    @Override
    public E dequeue() {
        return null;
    }
}
