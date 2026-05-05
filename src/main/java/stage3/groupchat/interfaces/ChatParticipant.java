package stage3.groupchat.interfaces;

public interface ChatParticipant {
    String getUsername();
    String getClientIP();
    void sendToClient(String message);
}
