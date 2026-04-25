package stage3.status;


/**
 * This class is a test for the StatusInterface implementations.
 * It simulates a scenario where multiple users are joining, leaving, and updating their statuses concurrently.
 * The test creates a number of threads that perform operations on the status object, and then checks the final list of statuses to see if it is consistent with the expected results.
 * The SafeStatus implementation should maintain a consistent state without any exceptions,
 * while the UnsafeStatus implementation may produce inconsistent results and throw
 *
 * @author John
 */
public class StatusTest {

    private void runTest(StatusInterface status) throws InterruptedException {

        int users = 500;
        int threads = 1000;

        for (int i = 0; i < users; i++) {
            status.userJoined("U" + i);
        }

        Thread[] ts = new Thread[threads];
        final int[] exceptions = {0}; // basically just int exceptions = 0

        for (int i = 0; i < threads; i++) {

            ts[i] = new Thread(() -> {
                try {
                    for (int ops = 0; ops < 5000; ops++) {
                        String username = "U" + (ops % users);

                        status.userLeft(username);
                        status.userJoined(username);
                        status.setStatus(username, (ops % 2 == 0) ? "test" : "away");
                    }
                } catch (Exception e) {
                    exceptions[0]++;
                }
            });

            ts[i].start();
        }

        for (Thread t : ts) t.join();

        // System.out.println(status.listStatuses());
        int userCount = countUsers(status.listStatuses());
        System.out.println("Exceptions: " + exceptions[0]);
        System.out.println("Status list length: " + userCount);

        boolean hasPassed = exceptions[0] == 0 && userCount == users;
        System.out.println(hasPassed ? "PASSED" : "FAILED");
    }

    public static void main(String[] args) throws InterruptedException {
        StatusTest status = new StatusTest();
        System.out.println("Testing Safe Status class...");
        status.runTest(new SafeStatus());
        System.out.println("\nTesting UnSafe Status class...");
        status.runTest(new UnsafeStatus());
    }

    private static int countUsers(String s) {
        int count = 0;
        for (String line : s.split("\\R")) { // this R is basically just \n but for all OS'
            if (line.contains("[") && line.contains("]")) count++;
        }
        return count;
    }
}