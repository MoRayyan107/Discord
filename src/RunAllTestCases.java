public class RunAllTestCases {

    /**
     * This class will run all the Test Cases for the GroupChat implementation, Future Test Cases can be called Here
     * @param args
     */
    public static void main(String[] args) {

        // --------------- Status Test ---------------
        System.out.println("-------------------------- Running StatusTest--------------------------");
        try{
            stage3.status.StatusTest.main(args);
            System.out.println();
        } catch (InterruptedException e) {
            System.out.println("StatusTest was interrupted: " + e.getMessage());
        }

    }
}
