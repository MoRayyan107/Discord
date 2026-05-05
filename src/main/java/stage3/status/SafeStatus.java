package stage3.status;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**  SAFE STATUS
 * This class implements the StatusInterface and provides thread-safe methods to manage user statuses in a chat application.
 * It uses a HashMap to store the statuses of users and a ReentrantLock to ensure that only one thread can access or modify the statuses at a time,
 * preventing race conditions and ensuring data consistency.
 *
 * @author John
 */
public class SafeStatus implements StatusInterface {

    private final Map<String, String> statuses;
    private final Lock lock;

    public SafeStatus() {
        statuses = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * When users joins the Server default status is "Online"
     * uses Lock to prevent multiple threads from modifying the statuses map at the same time,
     * handles race conditions
     * @param username the Client username to be set to "Online"
     */
    @Override
    public void userJoined(String username) {
        lock.lock();
        try {
            statuses.put(username, "online");
        } finally {
            lock.unlock();
        }
    }

    /**
     * remove the user form status list
     * uses Lock to prevent multiple threads from modifying the statuses map at the same time,
     *
     * @param username Client user to be removed from the status list
     */
    @Override
    public void userLeft(String username) {
        lock.lock();
        try {
            statuses.remove(username);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Sets a custom status message for a cleint
     * uses Lock to prevent multiple threads from modifying the statuses map at the same time
     *
     * @param username the Clients username that needs to be updated
     * @param status the custom Status message that the client wants to set
     */
    @Override
    public void setStatus(String username, String status) {
        lock.lock();
        try {
            statuses.put(username, status);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Lists all the users and their Status messages
     * uses Lock to prevent multiple threads from accessing the statuses map at the same time,
     * ensures that the returned list of statuses is consistent and not affected by concurrent modifications
     *
     * @return list of status
     */
    @Override
    public String listStatuses() {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder("STATUSES:\n");
            for (String user : statuses.keySet()) {
                sb.append(user)
                        .append(" [")
                        .append(statuses.get(user))
                        .append("]\n");
            }
            return sb.toString();
        } finally {
            lock.unlock();
        }
    }
}