package stage3.status;

import java.util.HashMap;
import java.util.Map;

/**   UNSAFE STATUS
 * This class is not thread-safe. It is used to demonstrate the issues that can arise when multiple threads access shared data without proper synchronization.
 * The statuses map is accessed and modified by multiple threads without any locks, which can lead to race conditions and inconsistent states.
 * For example, if two threads try to update the status of the same user at the same time, it can result in one update being lost or the map being corrupted.
 *
 * @author John
 */
public class UnsafeStatus implements StatusInterface {

    private final Map<String, String> statuses = new HashMap<>();

    /**
     * When users joins the Server default status is "Online"
     * @param username the Client user to be set to "Online"
     */
    @Override
    public void userJoined(String username) {
        statuses.put(username, "online");
    }

    /**
     * remove the user form status list
     * @param username Client user to be removed from the status list
     */
    @Override
    public void userLeft(String username) {
        statuses.remove(username);
    }

    /**
     * Sets a custom status message for a cleint
     * @param username the Clients username that needs to be updated
     * @param status the custom Status message that the client wants to set
     */
    @Override
    public void setStatus(String username, String status) {
        statuses.put(username, status);
    }

    /**
     * Lists all the users and their Status messages
     * @return list of status
     */
    @Override
    public String listStatuses() {
        StringBuilder sb = new StringBuilder("STATUSES:\n");
        for (String user : statuses.keySet()) {
            Thread.yield();
            sb.append(user)
                    .append(" [")
                    .append(statuses.get(user))
                    .append("]\n");
        }
        return sb.toString();
    }
}