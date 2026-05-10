import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import stage3.dm.DirectMessageInterface;
import stage3.dm.DirectServerMessage;

import static org.junit.jupiter.api.Assertions.*;

public class DirectMessageTest {

    DirectMessageInterface dmService = new DirectServerMessage();

    @Test
    void test_CreateDM_Success() {
        boolean created = dmService.createDirectMessage("alice", "bob");
        assertTrue(created);
    }

    @Test
    void test_CreateDM_NullSender() {
        boolean created = dmService.createDirectMessage(null, "bob");
        assertFalse(created);
    }

    @Test
    void test_CreateDM_NullReceiver() {
        boolean created = dmService.createDirectMessage("alice", null);
        assertFalse(created);
    }

    @Test
    void test_CreateDM_BothNull() {
        boolean created = dmService.createDirectMessage(null, null);
        assertFalse(created);
    }

    @Test
    void test_CheckDM_ExistsAfterCreation() {
        dmService.createDirectMessage("userA", "userB");
        assertTrue(dmService.checkIfDirectMessageExists("userA", "userB"));
    }

    @Test
    void test_CheckDM_OrderIndependent() {
        dmService.createDirectMessage("charlie", "dave");
        assertTrue(dmService.checkIfDirectMessageExists("dave", "charlie"));
    }

    @Test
    void test_CheckDM_NotExists() {
        assertFalse(dmService.checkIfDirectMessageExists("nonexistent1", "nonexistent2"));
    }

    @Test
    void test_CheckDM_NullSender() {
        assertFalse(dmService.checkIfDirectMessageExists(null, "bob"));
    }

    @Test
    void test_CheckDM_NullReceiver() {
        assertFalse(dmService.checkIfDirectMessageExists("alice", null));
    }

    @Test
    void test_DeleteDM_Success() {
        dmService.createDirectMessage("eve", "frank");
        assertTrue(dmService.checkIfDirectMessageExists("eve", "frank"));

        boolean deleted = dmService.deleteDirectMessage("eve", "frank");
        assertTrue(deleted);
        assertFalse(dmService.checkIfDirectMessageExists("eve", "frank"));
    }

    @Test
    void test_DeleteDM_OrderIndependent() {
        dmService.createDirectMessage("grace", "heidi");
        boolean deleted = dmService.deleteDirectMessage("heidi", "grace");
        assertTrue(deleted);
        assertFalse(dmService.checkIfDirectMessageExists("grace", "heidi"));
    }
}
