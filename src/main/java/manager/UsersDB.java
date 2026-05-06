package manager;

import org.mindrot.jbcrypt.BCrypt;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UsersDB {

    private final Connection connection;
    private final Properties prop;

    public UsersDB() throws SQLException {
        prop = new Properties();
        try{
            prop.load(getClass().getClassLoader().getResourceAsStream("db.properties"));
        } catch (IOException ex) {
            Logger.getLogger(UsersDB.class.getName()).log(Level.SEVERE, null, ex);
            throw new IllegalStateException(ex);
        }

        connection = connectDB();
        createDB(connection);
        createTable(connection);
    }


    private Connection connectDB() throws SQLException {
        String url = prop.getProperty("db.url");
        String user = prop.getProperty("db.username");
        String password = prop.getProperty("db.password");

        return DriverManager.getConnection(url, user, password);
    }

    private void createDB(Connection conn) throws SQLException {
        try {
            String query = "CREATE DATABASE \"DiscordApp\";";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeUpdate();
        } catch (SQLException ex) {
            if (ex.getSQLState().equals("42P04")) { // Database already exists
                System.out.println("Database already exists, skipping creation.");
            } else {
                throw ex; // Rethrow other SQL exceptions
            }
        }
    }

    private void createTable(Connection conn) throws SQLException {
        String query = "CREATE TABLE IF NOT EXISTS users (\n" +
                "    id SERIAL PRIMARY KEY,\n" +
                "    username VARCHAR(255) UNIQUE NOT NULL,\n" +
                "    email VARCHAR(255) UNIQUE NOT NULL,\n" +
                "    password VARCHAR(255) NOT NULL\n" +
                ");";

        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.executeUpdate();
        } catch (SQLException ex) {
            if (ex.getSQLState().equals("42P07")) { // Table already exists
                System.out.println("Table already exists, skipping creation.");
            } else {
                throw ex; // Rethrow other SQL exceptions
            }
        }
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

        if (!res.next()) return false;

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
