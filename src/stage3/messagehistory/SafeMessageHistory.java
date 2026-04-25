package stage3.messagehistory;

import java.util.ArrayList;
import java.util.List;

/**
 * safe version of message history
 * it uses synchronized on every method so only one thread
 * can access the list at a time. This prevents messages from being corrupted
 * when multiple users send at the same time
 * @author Ghassan
 */

public class SafeMessageHistory implements MessageHistory {

    private static final int MAX_MESSAGES = 1000;

    private final List<String> messages = new ArrayList<>();
    private final List<Long> timestamps = new ArrayList<>();

    /**
     * adds new messages to the history,
     * if the history exceeds MAX_MESSAGES it will remove the oldest message
     * this uses Synchronisation to ensure that no messages are lost when multiple threads call this function
     *
     * @param message the message that needs to be added
     */
    @Override
    public synchronized void addMessage(String message) {
        messages.add(message);
        timestamps.add(System.currentTimeMillis());

        if (messages.size() > MAX_MESSAGES) {
            messages.remove(0);
            timestamps.remove(0);
        }
    }

    /**
     * This strips out messages that were sent after the given timestamp.
     * This is used to get the messages that were sent while a user was offline.
     * uses Synchronisation to ensure that no messages are lost when multiple threads call this function
     *
     * @param since timestamp in ms (System.currentTimeMillis())
     * @return messages with timestamps
     */
    @Override
    public synchronized List<String> getMessagesSince(long since) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < timestamps.size(); i++) {
                if (timestamps.get(i) > since) {
                    result.add(messages.get(i));
                }
        }
        return  result;
    }

    /**
     * displays recent messages once joined
     * uses Synchronisation to ensure that no messages are lost when multiple threads call this function
     *
     * @return a listy of messages in the history
     */
    @Override
    public synchronized List<String> getRecentMessages() {
        return new ArrayList<>(messages);
    }
}
