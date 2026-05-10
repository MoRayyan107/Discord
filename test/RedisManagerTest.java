import manager.RedisManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class RedisManagerTest {

    private RedisManager redis;
    
    private static final String SERVER_A = "serverA";
    private static final String SERVER_B = "serverB";
    private static final String TEST_GROUP_NAME = "testGroup";

    @BeforeEach
    void setUp() {
        redis = new RedisManager();
    }

    // -------- Group Tests --------

    @Test
    void test_CreateGroup_Success() {
        try {
            boolean created = redis.createGroup(TEST_GROUP_NAME, SERVER_A);
            assertTrue(created);
        } finally {
            // Clean up after test
            redis.deleteGroup(TEST_GROUP_NAME, TEST_GROUP_NAME);
        }
    }

    @Test
    void test_CreateGroup_DuplicateFails() {
        try {

            redis.createGroup(TEST_GROUP_NAME, SERVER_A);
            boolean createdAgain = redis.createGroup(TEST_GROUP_NAME, SERVER_A);
            assertFalse(createdAgain);
        } finally {
            // Clean up after test
            redis.deleteGroup(TEST_GROUP_NAME, SERVER_A);
        }
    }

    @Test
    void test_GroupExists_True() {
        try {
            redis.createGroup(TEST_GROUP_NAME, SERVER_A);
            assertTrue(redis.groupExists(TEST_GROUP_NAME));
        } finally {
            // Clean up after test
            redis.deleteGroup(TEST_GROUP_NAME, SERVER_A);
        }
    }

    @Test
    void test_GroupExists_False() {
        assertFalse(redis.groupExists("nonExistentGroup"));
    }

    @Test
    void test_GetGroupServerID() {
        try {
            redis.createGroup(TEST_GROUP_NAME, SERVER_A);
            assertEquals(SERVER_A, redis.getGroupServerID(TEST_GROUP_NAME));
        } finally {
            // Clean up after test
            redis.deleteGroup(TEST_GROUP_NAME, SERVER_A);
        }
    }

    @Test
    void test_GetGroupServerID_NonExistent() {
        assertEquals("", redis.getGroupServerID("noSuchGroup"));
    }

    @Test
    void test_JoinGroup_Success() {
        try {
            redis.createGroup(TEST_GROUP_NAME, SERVER_A);
            boolean joined = redis.joinGroup(TEST_GROUP_NAME, "alice", SERVER_A);
            assertTrue(joined);
        } finally {
            // Clean up after test
            redis.deleteGroup(TEST_GROUP_NAME, SERVER_A);
        }
    }

    @Test
    void test_JoinGroup_NonExistentGroup() {
        boolean joined = redis.joinGroup("ghostGroup", "alice", SERVER_A);
        assertFalse(joined);
    }

    @Test
    void test_LeaveGroup_Success() {
        try {
            redis.createGroup(TEST_GROUP_NAME, SERVER_A);
            redis.joinGroup(TEST_GROUP_NAME, "bob", SERVER_A);
            boolean left = redis.leaveGroup(TEST_GROUP_NAME, "bob", SERVER_A);
            assertTrue(left);
        } finally {
            // Clean up after test
            redis.deleteGroup(TEST_GROUP_NAME, SERVER_A);
        }
    }

    @Test
    void test_LeaveGroup_NonExistentGroup() {
        boolean left = redis.leaveGroup("ghostGroup", "bob", SERVER_A);
        assertFalse(left);
    }

    @Test
    void test_CrossServer_True() {
        try {
            redis.createGroup(TEST_GROUP_NAME, SERVER_A);
            redis.joinGroup(TEST_GROUP_NAME, "user1", SERVER_A);
            redis.joinGroup(TEST_GROUP_NAME, "user2", SERVER_B);
            assertTrue(redis.isGroupCrossServer(TEST_GROUP_NAME, SERVER_A));
        } finally {
            // Clean up after test
            redis.deleteGroup(TEST_GROUP_NAME, SERVER_A);
        }
    }

    @Test
    void test_CrossServer_False_SameServerOnly() {
        try {
            redis.createGroup(TEST_GROUP_NAME, SERVER_A);
            redis.joinGroup(TEST_GROUP_NAME, "user3", SERVER_A);
            redis.joinGroup(TEST_GROUP_NAME, "user4", SERVER_A);
            assertFalse(redis.isGroupCrossServer(TEST_GROUP_NAME, SERVER_A));
        } finally {
            // Clean up after test
            redis.deleteGroup(TEST_GROUP_NAME, SERVER_A);
        }
    }

    @Test
    void test_CrossServer_EmptyGroupName() {
        assertFalse(redis.isGroupCrossServer("", SERVER_A));
    }

    @Test
    void test_CrossServer_NonExistentGroup() {
        assertFalse(redis.isGroupCrossServer("noSuchGroup", SERVER_A));
    }

    // -------- DM Tests --------

    @Test
    void test_CreateDM_Success() {
        boolean created = RedisManager.createDirectMessage("dmAlice", "dmBob");
        assertTrue(created);
    }

    @Test
    void test_DMExists_True() {
        RedisManager.createDirectMessage("dmCharlie", "dmDave");
        assertTrue(RedisManager.directMessageExists("dmCharlie", "dmDave"));
    }

    @Test
    void test_DMExists_OrderIndependent() {
        RedisManager.createDirectMessage("dmEve", "dmFrank");
        assertTrue(RedisManager.directMessageExists("dmFrank", "dmEve"));
    }

    @Test
    void test_DMExists_False() {
        assertFalse(RedisManager.directMessageExists("noDmUser1", "noDmUser2"));
    }

    @Test
    void test_DeleteDM_Success() {
        RedisManager.createDirectMessage("dmGrace", "dmHeidi");
        boolean deleted = RedisManager.deleteDirectMessage("dmGrace", "dmHeidi");
        assertTrue(deleted);
        assertFalse(RedisManager.directMessageExists("dmGrace", "dmHeidi"));
    }
}
