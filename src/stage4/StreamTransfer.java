package stage4;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;

public class StreamTransfer {

    private final String host;
    private final int port;
    private final File videoFile;

    /**
     * Constructor for StreamTransfer
     * @param host the IP address of Receiver
     * @param port the port that listens on
     * @param videoFile file to send
     */
    public StreamTransfer(String host, int port, File videoFile) {
        this.host = host;
        this.port = port;
        this.videoFile = videoFile;
    }

    /**
     * This method establishes a connection to the Receiver and streams the video file in chunks.
     */
    public void send() {
        Socket socket = null;
        FileInputStream fileIn = null;
        DataOutputStream out = null;

        byte[] buffer = new byte[64 * 1024]; // 64kb

        try {
            socket = new Socket(host, port);
            System.out.println("[SENDER] Connected to " + host + ":" + port);

            out = new DataOutputStream(socket.getOutputStream());
            fileIn = new FileInputStream(videoFile);

            int bytesRead;
            while ((bytesRead = fileIn.read(buffer)) != -1) {
                out.writeInt(bytesRead);
                out.write(buffer, 0, bytesRead);
                out.flush();
            }

            out.writeInt(-1);
            out.flush();

            System.out.println("[SENDER] Done streaming " + videoFile.getName());

        } catch (Exception e) {
            System.out.println("[SENDER] Error: " + e.getMessage());
        } finally {
            try { if (fileIn != null) fileIn.close(); } catch (IOException ignored) {}
            try { if (out != null) out.close(); } catch (IOException ignored) {}
            try { if (socket != null) socket.close(); } catch (IOException ignored) {}
        }
    }
}