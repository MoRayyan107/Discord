package stage3.dm;

public interface DirectMessageInterface {
    boolean createDirectMessage(String senderUsername, String receiverUsername);
    boolean checkIfDirectMessageExists(String senderUsername, String receiverUsername);

    // if ur a bad person youll use this
    boolean deleteDirectMessage(String senderUsername, String receiverUsername);
}
