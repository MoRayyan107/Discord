package stage3.friend;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/** SAFE FRIEND MANAGER
 * This implementation of FriendManager is thread-safe. It uses a combination of a global lock for the users map and individual locks for each UserNode to ensure that concurrent access to shared data is properly synchronized.
 * The mapLock is used to protect access to the users map when adding or removing users, while the individual locks in each UserNode are used to protect access to the friends set when adding friends.
 *
 * @author Ethan
 */
public class SafeFriendManager implements FriendInterface {

    private final Map<String, UserNode> users = new HashMap<>();
    private final Lock mapLock = new ReentrantLock();

    /**
     * initialises a user in the users map with a new UserNode if they do not already exist.
     * This method is thread-safe because it uses a lock to ensure that only one thread can modify the users map at a time.
     *
     * @param username the username of the user to be initialized in the users map
     */
    @Override
    public void initUser(String username) {
        mapLock.lock();
        try {
            users.putIfAbsent(username, new UserNode(username));
        } finally {
            mapLock.unlock();
        }
    }

    /**
     * removes a user from the users map.
     * This method is thread-safe because it uses a lock to ensure that only one thread can modify the users map at a time.
     *
     * @param username the username of the user to be removed from the users map
     */
    @Override
    public void removeUser(String username) {
        mapLock.lock();
        try {
            users.remove(username);
        } finally {
            mapLock.unlock();
        }
    }

    /**
     * retrieves the friends of a user from the users map.
     * This method is thread-safe because it uses a lock to ensure that
     * only one thread can access the users map at a time when retrieving the UserNode for the specified username.
     *
     * @param username the username of the user whose friends are to be retrieved from the users map
     * @return a String of Friends is displayed, else "No friends" if it does not exist
     */
    @Override
    public String getFriends(String username) {
        mapLock.lock();
        try {
            UserNode u = users.get(username);
            if (u != null && !u.friends.isEmpty()) {
                return u.friends.toString();
            } else {
                return "No friends";
            }
        } finally {
            mapLock.unlock();
        }
    }

    /**
     * adds a friend to the requester's friends list and adds the requester to the target's friends list.
     * This method is thread-safe because it uses a combination of locks to ensure that concurrent access to shared data is properly synchronized.
     * It first acquires the mapLock to retrieve the UserNodes for the requester and target,
     * and then it uses individual locks for each UserNode to protect access to the
     *
     * @param requester the Client user that sent the request
     * @param target the Client user that the requester wants to be friends with
     * @return a Success message if made friends, else an error message if not found/offline
     */
    @Override
    public String addFriend(String requester, String target) {
        if (requester.equals(target)) {
            return "You cannot friend yourself.";
        }
        UserNode reqNode;
        UserNode targetNode;

        mapLock.lock();
        try {
            reqNode = users.get(requester);
            targetNode = users.get(target);
        } finally {
            mapLock.unlock();
        }

        if (targetNode == null) {
            return "User '" + target + "' does not exist or is offline.";
        }
        if (reqNode == null) return "Error: Your data was not found.";

        // alphabetical lock ordering
        UserNode firstLock;
        UserNode secondLock;
        if (requester.compareTo(target) < 0) {
            firstLock = reqNode;
            secondLock = targetNode;
        } else {
            firstLock = targetNode;
            secondLock = reqNode;
        }
        firstLock.lock.lock();
        try {
            try {
                // keep delay so it's same as unsafe version to prove working
                Thread.sleep(50);
            } catch (InterruptedException ignored) {
            }

            secondLock.lock.lock();
            try {
                if (reqNode.friends.contains(target)) {
                    return "You are already friends with " + target + ".";
                }
                reqNode.friends.add(target);
                targetNode.friends.add(requester);
                return "Success! You and " + target + " are now friends.";

            } finally {
                secondLock.lock.unlock();
            }
        } finally {
            firstLock.lock.unlock();
        }
    }
}