import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import stage3.groupchat.SafeGroupChat;
import stage3.groupchat.UnsafeGroupChat;
import stage3.groupchat.interfaces.GroupChatInterfaces;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class GroupTest {

    GroupChatInterfaces safeChat = new SafeGroupChat("TestChat");
    GroupChatInterfaces unsafeChat = new UnsafeGroupChat("TestChat2");

    private static Thread[] threads;

    @BeforeAll
    static void init() {
        threads = new Thread[100];
    }

    private List<String> test_body(GroupChatInterfaces chat) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            final  int id = i;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    chat.sendMessage(null, "message from thread" + id);
                }
            });
            threads[i].start();
        }

        for (Thread t : threads){
            t.join();
        }
        // check the results
        return chat.getMessagesHistory();
    }

    @Test
    void test_SafeGroup() throws InterruptedException {
        List<String> history = test_body(safeChat);
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
        List<String> history = test_body(unsafeChat);
        System.out.println("Message history: " + history.size());

        long uniSeq = history.stream()
                .map(m -> m.split("#")[0].split(" ")[0])
                .distinct().count();
        System.out.println("Unique sequence numbers " + uniSeq);

        assertNotEquals(100, uniSeq);
    }

}
