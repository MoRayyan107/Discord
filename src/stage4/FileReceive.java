package stage4;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.*;
import java.io.DataInputStream;
import java.io.File;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;

public class FileReceive {
    static int fileId = 0;

    /**
     * gets the extension of Senders file
     *
     * @param fileName the received file from sender
     * @return the file extension as string
     */
    public static String getFileExtension(String fileName) {
        int i = fileName.lastIndexOf('.');
        if (i > 0) {
            return fileName.substring(i + 1).toLowerCase();
        } else {
            return "No extension found";
        }
    }

    /**
     * This creates a new Frame for the receiver
     * and starts a new thread that listens for incoming file transfers on specified port.
     *
     * @param port the port that Receiver listens on for incoming file transfers
     */
    public static void startReceiver(int port){
        JFrame jFrame = new JFrame("File");
        jFrame.setSize(400, 400);
        jFrame.setLayout(new BoxLayout(jFrame.getContentPane(), BoxLayout.Y_AXIS));
        jFrame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

        JPanel jPanel = new JPanel();
        jPanel.setLayout(new BoxLayout(jPanel, BoxLayout.Y_AXIS));

        JScrollPane jScrollPane = new JScrollPane(jPanel);
        jScrollPane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);

        JLabel jlTitle = new JLabel("File Receiver ");
        jlTitle.setFont(new Font("Arial", Font.BOLD, 25));
        jlTitle.setBorder(new EmptyBorder(20, 0, 10, 0));
        jlTitle.setAlignmentX(Component.CENTER_ALIGNMENT);

        jFrame.add(jlTitle);
        jFrame.add(jScrollPane);
        jFrame.setVisible(true);

        new Thread(() -> {
            try {
                ServerSocket serverSocket = new ServerSocket(port);
                System.out.println("Listening for file on port: " + port);
                while (true) {
                    Socket socket = serverSocket.accept();
                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());

                    int fileNameLength = dataInputStream.readInt();

                    if (fileNameLength > 0) {
                        byte[] fileNameBytes = new byte[fileNameLength];
                        dataInputStream.readFully(fileNameBytes, 0, fileNameBytes.length);
                        String fileName = new String(fileNameBytes);

                        System.out.println("Incoming file:  " + fileName);

                        int fileContentLength = dataInputStream.readInt();

                        if (fileContentLength > 0) {
                            byte[] fileContentBytes = new byte[fileContentLength];
                            dataInputStream.readFully(fileContentBytes, 0, fileContentLength);

                            JPanel jpFileRow = new JPanel();
                            jpFileRow.setLayout(new BoxLayout(jpFileRow, BoxLayout.Y_AXIS));

                            JLabel jlFileName = new JLabel(fileName);
                            jlFileName.setFont(new Font("Arial", Font.BOLD, 25));
                            jlFileName.setBorder(new EmptyBorder(10, 0, 10, 0));

                            if (getFileExtension(fileName).equalsIgnoreCase("txt") || getFileExtension(fileName).equalsIgnoreCase("pdf")) {
                                jpFileRow.setName(String.valueOf(fileId));

                                JButton OpenButton = new JButton("open " + fileName);
                                OpenButton.setFont(new Font("Arial", Font.PLAIN, 12));
                                OpenButton.addActionListener(e -> {
                                    try{
                                        File savedFile = new File(fileName);
                                        Files.write(savedFile.toPath(), fileContentBytes);

                                        Desktop.getDesktop().open(savedFile);
                                    } catch (Exception error) {
                                        error.printStackTrace();
                                    }
                                });

                                jpFileRow.add(jlFileName);
                                jpFileRow.add(OpenButton);
                                jPanel.add(jpFileRow);
                                jFrame.validate();

                            } else if (getFileExtension(fileName).equalsIgnoreCase("png") || getFileExtension(fileName).equalsIgnoreCase("jpg")) {
                                ImageIcon imageIcon = new ImageIcon(fileContentBytes);
                                Image image = imageIcon.getImage().getScaledInstance(350, 200, Image.SCALE_SMOOTH);
                                JLabel imageLabel = new JLabel(new ImageIcon(image));
                                imageLabel.setAlignmentX(Component.CENTER_ALIGNMENT);

                                jpFileRow.add(jlFileName);
                                jpFileRow.add(imageLabel);
                                jPanel.add(jpFileRow);
                                jFrame.validate();
                            } else if (getFileExtension(fileName).equalsIgnoreCase("mp4")){
                                try{
                                    File videoFile = new File(fileName);
                                    Files.write(videoFile.toPath(), fileContentBytes);
                                    Desktop.getDesktop().open(videoFile);

                                    jpFileRow.add(jlFileName);
                                    jPanel.add(jpFileRow);
                                    jFrame.validate();

                                } catch (Exception error) {
                                    error.printStackTrace();
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }
}
