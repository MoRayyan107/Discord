import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import stage3.messagehistory.MessageHistory;
import stage3.messagehistory.SafeMessageHistory;
import stage3.messagehistory.UnsafeMessageHistory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MessageHistoryTest {

    MessageHistory safeHistory = new SafeMessageHistory();
    MessageHistory unsafeHistory = new UnsafeMessageHistory();

    private static Thread[] threads;

    @BeforeAll
    static void init() {
        threads = new Thread[1000];
    }

    private List<String> test_body(MessageHistory messageHistory) throws InterruptedException {
        for (int i = 0; i < 1000; i++) {
            final int id = i;
            threads[i] = new Thread(() -> messageHistory.addMessage("Message from thread " + id));
            threads[i].start();
        }

        // wait for all threads to finish before counting
        for (Thread t : threads) {
            t.join();
        }

        return messageHistory.getRecentMessages();
    }

    @Test
    void testMessageHistory() throws InterruptedException {
        List<String> messages = test_body(safeHistory);
        System.out.println("Stored: " + messages.size() + " / 1000");

        assertEquals(1000, messages.size());
    }

    @Test
    void test_UnsafeGistory() throws InterruptedException {
        List<String> messages = test_body(unsafeHistory);
        System.out.println("Stored: " + messages.size() + " / 1000 (non-deterministic results can vary)");

        assert true;
    }
}
