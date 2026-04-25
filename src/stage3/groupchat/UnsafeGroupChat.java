package stage3.groupchat;

import stage3.groupchat.interfaces.ChatParticipant;
import stage3.groupchat.interfaces.GroupChatInterfaces;

import java.util.ArrayList;
import java.util.List;

/**
 * Unsafe classes because no synchronization or locks
 * Array lists with no thread safety
 * two threads can read the same value
 *
 * @author Mohamed Sharif
 */

public class UnsafeGroupChat implements GroupChatInterfaces {
    private final String name;
    private final List<ChatParticipant> members = new ArrayList<>();
    private final List<String> messageHistory = new ArrayList<>();
    private int sequenceCounter = 0;

    /**
     * Create a new group chat with the given name.
     * @param name the Group name
     */
    public UnsafeGroupChat(String name) { this.name = name; }

    @Override
    public String getName() { return name; }

    /**
     * adds new Client to the group
     * @param client the client object to be added to the group
     */
    @Override
    public void addMember(ChatParticipant client) {
        members.add(client);
        broadcast("SERVER", client.getUsername() + " joined " + name);
    }

    /**
     * removes a Client from the group
     * @param client the Client object that needs to be removed
     */
    @Override
    public void removeMember(ChatParticipant client) {
        members.remove(client);
        broadcast("Server", client.getUsername() + "left" + name);
    }

    /**
     * sends a message to the group,
     * this adds the message to the history and then broadcasts it to all members
     *
     * @param sender the Sender Client
     * @param message the message that needs to be delivered
     */
    @Override
    public void sendMessage(ChatParticipant sender, String message) {
        String senderName = (sender != null) ? sender.getUsername() : "TestThread";
        String format;
        sequenceCounter++;
        format = "[" + name + "] " + senderName + ": " + message;
        messageHistory.add(format);
        broadcast(senderName, format);
    }

    /**
     * broadcasts a message to all members of the group except the sender
     *
     * @param senderUsername senders username to avoid sending the message back to the sender
     * @param message message that needs to be broadcasted in group
     */
    @Override
    public void broadcast(String senderUsername, String message) {
        for (ChatParticipant member : members) {
            if (!member.getUsername().equals(senderUsername)) {
                member.sendToClient(message);
            }
        }
    }

    @Override
    public List<String> getMessagesHistory() {
        return new ArrayList<>(messageHistory); // return only live list
    }
}
