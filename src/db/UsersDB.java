package db;

import org.mindrot.jbcrypt.BCrypt;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UsersDB {

    private final Connection connection;

    public UsersDB() throws SQLException {
        connection = connectDB();
        createTable(connection);

    }

    private Connection connectDB() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:sqlite:chat.db");
        return conn;
    }

    private void createTable(Connection conn) throws SQLException {
        String query = "CREATE TABLE IF NOT EXISTS users (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "email TEXT NOT NULL," +
                "username TEXT UNIQUE NOT NULL," +
                "password TEXT NOT NULL" +
                ")";

        PreparedStatement statement = conn.prepareStatement(query);
        statement.execute();
    }

    public boolean register(String username, String email, String password) throws SQLException {
        String hashedPWD = BCrypt.hashpw(password, BCrypt.gensalt());

        String insert_Query = "INSERT INTO users (username, email, password) VALUES (?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(insert_Query);
        statement.setString(1, username);
        statement.setString(2, email);
        statement.setString(3, hashedPWD);

        int res = statement.executeUpdate();

        return res == 1;
    }


    public boolean login(String username, String password) throws SQLException {
        String fetch_hashed = "SELECT password FROM users WHERE username = ?";
        PreparedStatement statement = connection.prepareStatement(fetch_hashed);
        statement.setString(1, username);
        ResultSet res = statement.executeQuery();

        if (!res.next()) {
            return false;
        }
        String hashedPWD = res.getString("password");

        return  BCrypt.checkpw(password, hashedPWD);

    }

    public boolean deleteUser(String username) throws SQLException {
        String delete_Query = "DELETE FROM users WHERE username = ?";
        PreparedStatement statement = connection.prepareStatement(delete_Query);
        statement.setString(1, username);

        int res = statement.executeUpdate();
        return res == 1;
    }

    public void closeDB_Connection(){
        try {
            connection.close();
        } catch (SQLException ex) {
            Logger.getLogger(UsersDB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
