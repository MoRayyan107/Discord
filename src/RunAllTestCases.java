import stage3.groupchat.GroupChatTest;
import stage3.messagehistory.MessageHistoryTest;

public class RunAllTestCases {

    /**
     * This class will run all the Test Cases for the GroupChat implementation, Future Test Cases can be called Here
     * @param args
     */
    public static void main(String[] args) {

        // --------------- Message History Test ---------------
        System.out.println("-------------------------- Running GroupChatTest --------------------------");
        try {
            GroupChatTest.main(args);
            System.out.println();
        } catch (InterruptedException e) {
            System.out.println("GroupChatTest was interrupted: " + e.getMessage());
        }

        // --------------- Message History Test ---------------
        System.out.println("-------------------------- Running MessageHistoryTest --------------------------");
        try {
            MessageHistoryTest.main(args);
            System.out.println();
        } catch (InterruptedException e) {
            System.out.println("MessageHistoryTest was interrupted: " + e.getMessage());
        }

        // --------------- Status Test ---------------
        System.out.println("-------------------------- Running StatusTest--------------------------");
        try{
            stage3.status.StatusTest.main(args);
            System.out.println();
        } catch (InterruptedException e) {
            System.out.println("StatusTest was interrupted: " + e.getMessage());
        }

        // --------------- Friend Test ---------------
        System.out.println("-------------------------- Running FriendTest--------------------------");
        try{
            stage3.friend.FriendTest.main(args);
            System.out.println();
        } catch (InterruptedException e){
            System.out.println("FriendTest was interrupted: " + e.getMessage());
        }
    }
}
