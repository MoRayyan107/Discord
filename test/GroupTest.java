import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import stage3.groupchat.SafeGroupChat;
import stage3.groupchat.UnsafeGroupChat;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class GroupTest {

    SafeGroupChat safeChat = new SafeGroupChat("TestChat");
    UnsafeGroupChat unsafeChat = new UnsafeGroupChat("TestChat2");

    @Test
    void test_SafeGroup() throws InterruptedException {
        Thread [] threads = new Thread[100];

        for (int i = 0; i < 100; i++) {
            final  int id = i;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    safeChat.sendMessage(null, "message from thread" + id);
                }
            });
            threads[i].start();
        }

        for (Thread t : threads){
            t.join();
        }
        // check the results
        List<String> history = safeChat.getMessagesHistory();
        System.out.println("Message history: " + history.size());

        long uniSeq = history.stream()
                .map(m -> m.split("#")[0].split(" ")[0])
                .distinct().count();
        System.out.println("Unique sequence numbers " + uniSeq);

        assertEquals(100, uniSeq);
        assertEquals(100, history.size());
    }

    @Test
    void test_UnsafeGroup() throws InterruptedException {
        Thread [] threads = new Thread[100];

        for (int i = 0; i < 100; i++) {
            final  int id = i;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    unsafeChat.sendMessage(null, "message from thread" + id);
                }
            });
            threads[i].start();
        }

        for (Thread t : threads){
            t.join();
        }
        // check the results
        List<String> history = unsafeChat.getMessagesHistory();
        System.out.println("Message history: " + history.size());

        long uniSeq = history.stream()
                .map(m -> m.split("#")[0].split(" ")[0])
                .distinct().count();
        System.out.println("Unique sequence numbers " + uniSeq);

        assertNotEquals(100, uniSeq);

    }

}
