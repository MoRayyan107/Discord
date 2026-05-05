import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import stage3.status.SafeStatus;
import stage3.status.StatusInterface;
import stage3.status.UnsafeStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class StatusTest {

    StatusInterface safeStatus = new SafeStatus();
    StatusInterface unsafeStatus = new UnsafeStatus();

    private static int users;
    private static int threads;

    @BeforeAll
    static void setup() {
        users = 500;
        threads = 1000;
    }

    public int[] test_body() throws InterruptedException {
        for (int i = 0; i < users; i++) {
            safeStatus.userJoined("U" + i);
        }

        Thread[] ts = new Thread[threads];
        final int[] exceptions = {0}; // basically just int exceptions = 0

        for (int i = 0; i < threads; i++) {

            ts[i] = new Thread(() -> {
                try {
                    for (int ops = 0; ops < 5000; ops++) {
                        String username = "U" + (ops % users);

                        safeStatus.userLeft(username);
                        safeStatus.userJoined(username);
                        safeStatus.setStatus(username, (ops % 2 == 0) ? "test" : "away");
                    }
                } catch (Exception e) {
                    exceptions[0]++;
                }
            });

            ts[i].start();
        }

        for (Thread t : ts) t.join();

        // System.out.println(status.listStatuses());
        int userCount = countUsers(safeStatus.listStatuses());

        return new int[]{userCount, exceptions[0]};
    }

    @Test
    public void statusTest_Pass() throws InterruptedException {
        int[] params = test_body();
        int userCount = params[0];
        int exceptions = params[1];
        System.out.println("Exceptions: " + exceptions);
        System.out.println("Status list length: " + userCount);

        assertEquals(0, exceptions);
        assertEquals(users, userCount);
    }

    @Test
    public void statusTest_Fail() throws InterruptedException {
        int[] params = test_body();
        int userCount = params[0];
        int exceptions = params[1];

        System.out.println("Exceptions: " + exceptions);
        System.out.println("Status list length: " + userCount);

        assertEquals(0, exceptions);
        assertEquals(users, userCount);
    }

    private static int countUsers(String s) {
        int count = 0;
        for (String line : s.split("\\R")) { // this R is basically just \n but for all OS'
            if (line.contains("[") && line.contains("]")) count++;
        }
        return count;
    }
}
