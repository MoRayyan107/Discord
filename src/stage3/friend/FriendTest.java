package stage3.friend;

/**
 * This test class demonstrates the potential for deadlocks in friend management systems.
 * It runs two threads that attempt to add each other as friends simultaneously.
 * The SafeFriendManager should handle this without deadlocking, while the UnsafeFriendManager may deadlock due to inconsistent lock ordering.
 *
 * @author Ethan
 */
public class FriendTest {
    public static void main(String[] args) throws InterruptedException {

        System.out.println("Testing SafeFriendManager...");
        runTest(new SafeFriendManager());

        Thread.sleep(1000);

        System.out.println("\nTesting UnsafeFriendManager...");
        runTest(new UnsafeFriendManager());
        System.exit(0);

    }

    private static void runTest(FriendInterface friendSystem) throws InterruptedException {
        friendSystem.initUser("user1");
        friendSystem.initUser("user2");

        // thread 1: user1 friends user2
        Thread t1 = new Thread(() -> {
            System.out.println("Thread 1: user1 is trying to friend user2...");
            friendSystem.addFriend("user1", "user2");
            System.out.println("Thread 1: Success!");
        });

        // thread 2: user2 friends user1
        Thread t2 = new Thread(() -> {
            System.out.println("Thread 2: Bob is trying to friend user1...");
            friendSystem.addFriend("user2", "user1");
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
            System.out.println("user1's friends: " + friendSystem.getFriends("user1"));
            System.out.println("user2's friends: " + friendSystem.getFriends("user2"));
        }
    }
}