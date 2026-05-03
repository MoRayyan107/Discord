import org.junit.jupiter.api.Test;
import stage3.messagehistory.SafeMessageHistory;
import stage3.messagehistory.UnsafeMessageHistory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class MessageHistoryTest {

    SafeMessageHistory safeHistory = new SafeMessageHistory();
    UnsafeMessageHistory unsafeHistory = new UnsafeMessageHistory();

    @Test
    void testMessageHistory() throws InterruptedException {

        Thread[] threads = new Thread[1000];

        for (int i = 0; i < 1000; i++) {
            final int id = i;
            threads[i] = new Thread(() -> safeHistory.addMessage("Message from thread " + id));
            threads[i].start();
        }

        // wait for all threads to finish before counting
        for (Thread t : threads) {
            t.join();
        }

        List<String> messages = safeHistory.getRecentMessages();
        System.out.println("Stored: " + messages.size() + " / 1000");

        assertEquals(1000, messages.size());
    }

    @Test
    void test_UnsafeGistory() throws InterruptedException {
        Thread[] threads = new Thread[1000];

        for (int i = 0; i < 1000; i++) {
            final int id = i;
            threads[i] = new Thread(() -> unsafeHistory.addMessage("Message from thread " + id));
            threads[i].start();
        }

        // wait for all threads to finish before counting
        for (Thread t : threads) {
            t.join();
        }

        List<String> messages = unsafeHistory.getRecentMessages();
        System.out.println("Stored: " + messages.size() + " / 1000 (non-deterministic results can vary)");

        assert true;
    }
}
