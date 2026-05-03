import org.junit.jupiter.api.Test;
import stage3.friend.SafeFriendManager;
import stage3.friend.UnsafeFriendManager;

import static org.junit.Assert.*;

public class FriendTest {

    SafeFriendManager safeManager =  new SafeFriendManager();
    UnsafeFriendManager unsafeManager =  new UnsafeFriendManager();

    @Test
    void test_SafeFriend() throws InterruptedException {
        safeManager.initUser("user1");
        safeManager.initUser("user2");

        // thread 1: user1 friends user2
        Thread t1 = new Thread(() -> {
            System.out.println("Thread 1: user1 is trying to friend user2...");
            safeManager.addFriend("user1", "user2");
            System.out.println("Thread 1: Success!");
        });

        // thread 2: user2 friends user1
        Thread t2 = new Thread(() -> {
            System.out.println("Thread 2: Bob is trying to friend user1...");
            safeManager.addFriend("user2", "user1");
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
            System.out.println("user1's friends: " + safeManager.getFriends("user1"));
            System.out.println("user2's friends: " + safeManager.getFriends("user2"));

        }
        assertNotNull(safeManager.getFriends("user1"));
    }

    @Test
    void test_UnsafeFriend() throws InterruptedException {
        unsafeManager.initUser("user1");
        unsafeManager.initUser("user2");

        // thread 1: user1 friends user2
        Thread t1 = new Thread(() -> {
            System.out.println("Thread 1: user1 is trying to friend user2...");
            unsafeManager.addFriend("user1", "user2");
            System.out.println("Thread 1: Success!");
        });

        // thread 2: user2 friends user1
        Thread t2 = new Thread(() -> {
            System.out.println("Thread 2: Bob is trying to friend user1...");
            unsafeManager.addFriend("user2", "user1");
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
            System.out.println("user1's friends: " + unsafeManager.getFriends("user1"));
            System.out.println("user2's friends: " + unsafeManager.getFriends("user2"));

        }

        assertEquals(unsafeManager.getFriends("user1"), "No Friends");
        assertEquals(unsafeManager.getFriends("user2"), "No Friends");
    }
}
