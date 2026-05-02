package stage3.groupchat;

import stage3.groupchat.interfaces.ChatParticipant;
import stage3.groupchat.interfaces.GroupChatInterfaces;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SafeGroupChat implements GroupChatInterfaces {
    private final String name;
    private final List<ChatParticipant> members = new ArrayList<>();
    private final List<String> messageHistory = new ArrayList<>();
    private final Lock the_lock = new ReentrantLock(); // ReentrantLock gives more control than the synchronized

    // This is a thread safe way to generate sequence numbers without needing to synchronize the entire method
    private final AtomicInteger sequenceNumber = new AtomicInteger(0);

    // using a blocking queue for sending messages so user thread always listen than working on sending messages
    private final BlockingQueue<String[]> messagesQueue = new LinkedBlockingQueue<>();

    public SafeGroupChat(String name) {
        this.name = name;

        Thread dispatcherThread = new Thread(() -> {
            while (true) {
                try {
                    String[] msgArray = messagesQueue.take(); // 0 -> username and 1 -> messgae
                    broadcast(msgArray[0], msgArray[1]);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        // kills when the server is down
        dispatcherThread.setDaemon(true);
        dispatcherThread.start();

    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * adds new Client to the group
     * uses locks to ensure that only one thread can modify the members list at a time,
     * hence preventing concurrent modification issues
     *
     * @param client the client object to be added to the group
     */
    @Override
    public void addMember(ChatParticipant client) {
        try {
            the_lock.lock();
            members.add(client);
        } finally {
            the_lock.unlock();
        }
        // happens outside the lock which is same as Thread.sleep()
        broadcast("SERVER", client.getUsername() + " joined " + name);
    }

    /**
     * removes a Client from the group
     * uses Locks to ensure that only one thread can modify the members list at a time,
     * hence preventing concurrent modification issues
     *
     * @param client the Client object that needs to be removed
     */
    @Override
    public void removeMember(ChatParticipant client) {
        try{
            the_lock.lock();
            members.remove(client);
        } finally {
            the_lock.unlock();
        }
        broadcast("Server", client.getUsername() + " left" + name);
    }

    /**
     * sends a message to the group,
     * this adds the message to the history and then broadcasts it to all members
     * uses Locks to ensure that only one thread can modify the message history at a time,
     *
     * @param sender the Sender Client
     * @param message the message that needs to be delivered
     */
    @Override
    public void sendMessage(ChatParticipant sender, String message) {
        String senderName = (sender != null) ? sender.getUsername() : "TestThread";
        String format;
        String actualMsg;
        messagesQueue.offer(new String[]{senderName, message}); // better than put (doesnt throw any issues is the queue is full)

        try {
            the_lock.lock();
            actualMsg = senderName + ": " + message;
            format = sequenceNumber.getAndIncrement() + "[" + name + "] " + senderName + ": " + message;
            messageHistory.add(format);
        } finally {
            the_lock.unlock();
        }
    }

    /**
     * broadcasts a message to all members of the group except the sender
     * uses Locks to ensure that only one thread can read the members list at a time,
     *
     * @param senderUsername senders username to avoid sending the message back to the sender
     * @param message message that needs to be broadcasted in group
     */
    @Override
    public void broadcast(String senderUsername, String message) {
        List <ChatParticipant> membersCopy;
        try{
            the_lock.lock();
            membersCopy = new ArrayList<>(members);
        } finally {
            the_lock.unlock();
        }
        for (ChatParticipant member : membersCopy) {
            if (!member.getUsername().equals(senderUsername)) {
                member.sendToClient(message);
            }
        }
    }

    @Override
    public List<String> getMessagesHistory() {
        try{
            the_lock.lock();
            return new ArrayList<>(messageHistory);
        } finally {
            the_lock.unlock();
        }
    }
}
