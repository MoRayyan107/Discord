package stage3.groupchat;

import stage3.groupchat.interfaces.GroupChatInterfaces;

import java.util.List;

/**
 * This class is designed to test the thread safety of the SafeGroupChat and UnsafeGroupChat implementations.
 * It creates multiple threads that send messages concurrently to the group chat and then checks the message history
 * to ensure that all messages were sent and that each message has a unique sequence number.
 *
 * The test will print "Pass" if all messages are present and have unique sequence numbers, otherwise it will print "Fail".
 *
 * @author Mohamed Sharif
 */
public class GroupChatTest {

    public void runTest(GroupChatInterfaces chat) throws InterruptedException {

        Thread [] threads = new Thread[100];

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
        List<String> history = chat.getMessagesHistory();
        System.out.println("Message history: " + history.size());

        long uniSeq = history.stream()
                .map(m -> m.split("#")[0].split(" ")[0]) //FIXME: this throws and erros and changing to 0 will result for safe to fail too
                .distinct().count();

        System.out.println("Unique sequence numbers " + uniSeq);
        System.out.println(history.size() == 100 &&  uniSeq == 100 ? "Pass" : "Fail");
    }


    public static void main(String[] args) throws InterruptedException {

        GroupChatTest test = new GroupChatTest();
        System.out.println("Testing SafeGroupChat...");
        test.runTest(new SafeGroupChat("TestChat"));

        System.out.println("\nTesting UnsafeGroupChat...");
        test.runTest(new UnsafeGroupChat("TestChat"));
    }
}
