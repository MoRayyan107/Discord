package stage3.friend;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * UNSAFE FRIEND MANAGER
 * This implementation of FriendManager is NOT thread-safe. It does not use any synchronization mechanisms toq protect access to the shared users map or the friends sets within each UserNode
 * This can lead to race conditions and inconsistent states when multiple threads access and modify the users map or the friends sets concurrently.
 * For example, if two threads try to add the same friend to a user's friends set at the same time, it can result in one of the additions being lost or the friends set being corrupted.
 *
 * @author Ethan
 */
public class UnsafeFriendManager implements FriendInterface {

    private final Map<String, UserNode> users = new HashMap<>();

    private final Lock mapLock = new ReentrantLock();

    /**
     * initialises a user in the users map with a new UserNode if they do not already exist.
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
            }
            else {
                return "No friends";
            }
        } finally {
            mapLock.unlock();
        }
    }

    /**
     * adds a friend to the requester's friends list and adds the requester to the target's friends list.
     * This is where deadlock can occur because it locks the requester and target UserNodes without a consistent locking order.
     * If two threads try to add each other as friends at the same time, they can end up waiting on each other's locks, resulting in a deadlock.
     *
     * @param requester the username of the user who is sending the friend request
     * @param target the username of the user who is receiving the friend request
     * @return  a Success message if made friends, else an error message if not found/offline
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


        // no alphabetical locking
        reqNode.lock.lock();
        try {
            // delay to ensure deadlock
            try { Thread.sleep(50); } catch (InterruptedException ignored) {}
            if (reqNode.friends.contains(target)) {
                return "You are already friends with " + target + ".";
            }
            targetNode.lock.lock();
            try {
                reqNode.friends.add(target);
                targetNode.friends.add(requester);
            } finally {
                targetNode.lock.unlock();
            }
        } finally {
            reqNode.lock.unlock();
        }
        return "Success! You and " + target + " are now friends.";
    }
}