package stage3.groupchat.interfaces;

import java.util.List;

public interface GroupChatInterfaces {
    String getName();
    void addMember(ChatParticipant client);
    void removeMember(ChatParticipant client);
    void sendMessage(ChatParticipant sender, String message);
    void broadcast(String senderUsername, String message);
    List<String> getMessagesHistory();
}
