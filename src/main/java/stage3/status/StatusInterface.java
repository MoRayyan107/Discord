package stage3.status;

public interface StatusInterface {
    void userJoined(String username);
    void userLeft(String username);

    void setStatus(String username, String status);

    String listStatuses();
}