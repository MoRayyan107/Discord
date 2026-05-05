import db.UsersDB;
import org.junit.jupiter.api.*;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuthTest {

    private String username;
    private String password;
    private String email;
    private UsersDB users;

    @BeforeEach
    public void setUp() throws SQLException {
        users = new UsersDB();
        username = "testuser";
        password = "testpassword";
        email = "testemail";
    }

    @AfterEach
    public void tearDown() throws SQLException {
        users.deleteUser(username);
        users.closeDB_Connection();
    }



    @Test
    public void AuthTest_Pass() throws SQLException {
        try{
            users.register(username, email, password);

            assertTrue(users.login(username, password)) ;

        } catch (Exception e) {
            System.err.println("Failed to connect to DB: " + e.getMessage());
            assert false;
        }
    }

    @Test
    public void AuthTest_Fail_NoRegister() throws SQLException{
        try {
            // do not register anmd try login
            assertFalse(users.login(username, "wrongpass")) ;
        } catch (Exception e) {
            System.err.println("Failed to connect to DB: " + e.getMessage());
            assert false;
        }
    }

    @Test
    public void AuthTest_Fail_withRegistering() throws SQLException {
        try{

            users.register(username, email, password);

            assertFalse(users.login(username, "WrongPassword")) ;

        } catch (Exception e) {
            System.err.println("Failed to connect to DB: " + e.getMessage());
            assert false;
        }
    }
}
