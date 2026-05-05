import org.junit.jupiter.api.Test;
import stage3.friend.FriendInterface;
import stage3.friend.SafeFriendManager;
import stage3.friend.UnsafeFriendManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class FriendTest {

    FriendInterface safeManager =  new SafeFriendManager();
    FriendInterface unsafeManager =  new UnsafeFriendManager();

    private FriendInterface test_body(FriendInterface friend) throws InterruptedException {
        friend.initUser("user1");
        friend.initUser("user2");

        // thread 1: user1 friends user2
        Thread t1 = new Thread(() -> {
            System.out.println("Thread 1: user1 is trying to friend user2...");
            friend.addFriend("user1", "user2");
            System.out.println("Thread 1: Success!");
        });

        // thread 2: user2 friends user1
        Thread t2 = new Thread(() -> {
            System.out.println("Thread 2: Bob is trying to friend user1...");
            friend.addFriend("user2", "user1");
            System.out.println("Thread 2: Success!");
        });

        t1.start();
        t2.start();

        t1.join(3000);
        t2.join(3000);

        if (t1.isAlive() || t2.isAlive()) {
            System.err.println("Deadlocked: Exceeded time limit");
            t1.interrupt();
            t2.interrupt();
        } else {
            System.out.println("No Deadlock: Successfully added each other");
            System.out.println("user1's friends: " + friend.getFriends("user1"));
            System.out.println("user2's friends: " + friend.getFriends("user2"));

        }
        return friend;
    }

    @Test
    void test_SafeFriend() throws InterruptedException {
        FriendInterface friend = test_body(safeManager);
        assertNotNull(safeManager.getFriends("user1"));
    }

    @Test
    void test_UnsafeFriend() throws InterruptedException {
        FriendInterface friend = test_body(unsafeManager);
        assertEquals(unsafeManager.getFriends("user1"), "No Friends");
        assertEquals(unsafeManager.getFriends("user2"), "No Friends");
    }
}
