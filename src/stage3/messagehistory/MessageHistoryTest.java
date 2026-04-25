package stage3.messagehistory;

import java.util.List;


/**
 * Test class for MessageHistory implementations.
 * It creates 1000 threads that all add a message to the history at the same time, then checks how many messages were stored.
 * The SafeMessageHistory should always store all 1000 messages, while the UnsafeMessageHistory will often lose some messages due to thread interference.
 *
 * @author Ghassan
 */
public class MessageHistoryTest {

    private void runTest(MessageHistory history) throws InterruptedException {
        Thread[] threads = new Thread[1000];

        for (int i = 0; i < 1000; i++) {
            final int id = i;
            threads[i] = new Thread(() -> history.addMessage("Message from thread " + id));
            threads[i].start();
        }

        // wait for all threads to finish before counting
        for (Thread t : threads) {
            t.join();
        }

        List<String> messages = history.getRecentMessages();
        System.out.println("Stored: " + messages.size() + " / 1000");
        System.out.println(messages.size() == 1000 ? "PASSED" : "FAILED lost " + (1000 - messages.size()));

    }

    public static void main(String[] args) throws InterruptedException {

        MessageHistoryTest test = new MessageHistoryTest();
        System.out.println("Testing SafeMessageHistory...");
        test.runTest(new SafeMessageHistory());
        System.out.println("\nTesting UnsafeMessageHistory...");
        test.runTest(new UnsafeMessageHistory());
    }
}