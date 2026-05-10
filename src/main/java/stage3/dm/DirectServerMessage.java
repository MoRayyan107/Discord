package stage3.dm;

import manager.RedisManager;

public class DirectServerMessage implements DirectMessageInterface {

    @Override
    public boolean createDirectMessage(String senderUsername, String receiverUsername) {
        if (senderUsername == null || receiverUsername == null){
            System.err.println("Sender and Receiver both are null!");
            return false;
        }
        return RedisManager.createDirectMessage(senderUsername, receiverUsername);
    }

    @Override
    public boolean checkIfDirectMessageExists(String senderUsername, String receiverUsername) {
        if (senderUsername == null || receiverUsername == null) {
            System.err.println("Sender and Receiver both are null!");
            return false;
        }

        return RedisManager.directMessageExists(senderUsername, receiverUsername);
    }

    // horrific person you're dude
    @Override
    public boolean deleteDirectMessage(String senderUsername, String receiverUsername) {
        return RedisManager.deleteDirectMessage(senderUsername, receiverUsername);
    }
}
