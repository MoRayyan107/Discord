package stage3.messagehistory;

import java.util.ArrayList;
import java.util.List;

/**
 * UNSAFE version of message history.
 * Identical logic to SafeMessageHistory but with NO synchronization.
 * When multiple threads call addMessage() at the same time, the
 * ArrayList can get corrupted and messages get lost or the app crashes.
 * @author Ghassan
 */

public class UnsafeMessageHistory implements MessageHistory {

    private static final int MAX_MESSAGES = 1000;

    private final List<String> messages = new ArrayList<>();
    private final List<Long> timestamps = new ArrayList<>();

    /**
     * adds new messages to the history,
     * if the history exceeds MAX_MESSAGES it will remove the oldest message
     *
     * @param message the message that needs to be added
     */
    @Override
    public void addMessage(String message) {
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
     *
     * @param since timestamp in ms (System.currentTimeMillis())
     * @return messages with timestamps
     */
    @Override
    public List<String> getMessagesSince(long since) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < timestamps.size(); i++) {
            if (timestamps.get(i) > since) {
                result.add(messages.get(i));
            }
        }
        return result;
    }

    /**
     * displays recent messages once joined
     *
     * @return a listy of messages in the history
     */
    @Override
    public List<String> getRecentMessages() {
        return new ArrayList<>(messages);
    }
}