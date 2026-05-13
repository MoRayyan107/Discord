package stage3.groupchat.interfaces;

public interface ChatParticipant {
    String getUsername();
    String getClientIP();
    String getCurrentGroup();
    void sendToClient(String message);
}
