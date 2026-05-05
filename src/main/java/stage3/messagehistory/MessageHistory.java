package stage3.messagehistory;

import java.util.List;

/**
 * Interface for message history.
 * Both safe and unsafe implementations use this
 * so tests can swap between them easily.
 */
public interface MessageHistory {

    /**
     * Add a message to the history.
     * Only the last 100 messages are kept.
     */
    void addMessage(String message);

    /**
     * Get all messages sent after a given timestamp.
     * Used when a user reconnects — they only get what they missed.
     * @param since timestamp in ms (System.currentTimeMillis())
     */
    List<String> getMessagesSince(long since);

    /**
     * Get the last 100 messages.
     * Used for first-time connections.
     */
    List<String> getRecentMessages();
}
