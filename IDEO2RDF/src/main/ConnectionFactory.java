package main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {
    public Connection getConnection(String db, String username, String password) {
        try {
            return DriverManager.getConnection(
          db, username, password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}