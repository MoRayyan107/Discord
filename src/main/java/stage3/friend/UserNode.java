package stage3.friend;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class UserNode {
    public final String username;
    public final Set<String> friends = new HashSet<>();
    public final Lock lock = new ReentrantLock();

    public UserNode(String username) {
        this.username = username;
    }
}