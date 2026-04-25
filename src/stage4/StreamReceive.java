package stage4;

import java.awt.Desktop;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class StreamReceive {

    private final int listenPort;
    private final File outputFile = new File("received.ts");

    /**
     * Constructor for StreamReceive
     * @param listenPort the port that Receiver will listen on
     */
    public StreamReceive(int listenPort) {
        this.listenPort = listenPort;
    }

    /**
     * This method sets up a ServerSocket to listen for incoming video stream data.
     * It writes the incoming data to a temporary file and launches VLC/Media player to play it once enough data has been buffered
     */
    public void receiveAndPlay() {
        ServerSocket serverSocket = null;
        Socket socket = null;
        DataInputStream in = null;
        FileOutputStream fileOut = null;

        boolean startedPlayer = false;
        long bytesWritten = 0;

        try {
            if (outputFile.exists()) outputFile.delete();
            fileOut = new FileOutputStream(outputFile, true);

            serverSocket = new ServerSocket(listenPort);
            System.out.println("[RECEIVER] Listening on port " + listenPort);

            socket = serverSocket.accept();
            System.out.println("[RECEIVER] Sender connected: " + socket.getInetAddress());

            in = new DataInputStream(socket.getInputStream());

            while (true) {
                int chunkLen = in.readInt();
                if (chunkLen == -1) break;

                byte[] chunk = new byte[chunkLen];
                in.readFully(chunk);

                fileOut.write(chunk);
                fileOut.flush();
                bytesWritten += chunkLen;

                if (!startedPlayer && bytesWritten >= (1024 * 1024)) {
                    startedPlayer = true;
                    System.out.println("[RECEIVER] Starting VLC after buffering " + bytesWritten + " bytes");
                    launchVLC(outputFile);
                }
            }

            System.out.println("[RECEIVER] Stream ended. Total bytes written = " + bytesWritten);

            if (!startedPlayer) {
                System.out.println("[RECEIVER] File small; starting VLC now.");
                launchVLC(outputFile);
            }

        } catch (Exception e) {
            System.out.println("[RECEIVER] Error: " + e.getMessage());
        } finally {
            try { if (fileOut != null) fileOut.close(); } catch (IOException ignored) {}
            try { if (in != null) in.close(); } catch (IOException ignored) {}
            try { if (socket != null) socket.close(); } catch (IOException ignored) {}
            try { if (serverSocket != null) serverSocket.close(); } catch (IOException ignored) {}
        }
    }

    /**
     * helper function to launch VLC or the default media player with the given file
     * @param file the media file to be played
     */
    private void launchVLC(File file) {
        try {Desktop.getDesktop().open(file);} catch (Exception ignored) { }
    }

    public static void main(String[] args) {
        new StreamReceive(9000).receiveAndPlay();
    }
}