package stage3.friend;

public interface FriendInterface {
    void initUser(String username);
    void removeUser(String username);
    String addFriend(String requester, String target);
    String getFriends(String username);
}