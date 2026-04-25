package stage4;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;

public class FileTransfer {

    // when user does !sendfile
    // based on file transfer (pdf, txt, jpg, etc)
    public static void openSenderGUI(String targetIP, int port){
        SwingUtilities.invokeLater(() -> GUI(targetIP, port));
    }

    // based on video streaming files (ts, mp4)
    public static void openStreamGUI(String targetIP, int port) {
        SwingUtilities.invokeLater(() -> GUIStream(targetIP, port));
    }

    /**
     * This creates a new Frame for the sender, allowing them to choose a file and send it to the receiver.
     * The sender can choose any file type, and the receiver will save it with the same name and extension.
     *
     * @param targetIP IP to make bridge with
     * @param port designated port for that bridge
     */
    public static void GUI (String targetIP, int port) {

        final File [] fileToSend = new File[1];

        JFrame jFrame = new JFrame("File Transfer");
        jFrame.setSize(450,450);
        jFrame.setLayout(new BoxLayout(jFrame.getContentPane(), BoxLayout.Y_AXIS));
        jFrame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

        JLabel jTitle = new JLabel("File Sender");
        jTitle.setFont(new Font("Arial", Font.BOLD, 25));
        jTitle.setBorder(new EmptyBorder(20, 0, 10, 0));
        jTitle.setAlignmentX(Component.CENTER_ALIGNMENT);

        JLabel jFileName = new JLabel("Choose a file to send.");
        jFileName.setFont(new Font("Arial", Font.BOLD, 20));
        jFileName.setBorder(new EmptyBorder(50, 0, 0, 0));
        jFileName.setAlignmentX(Component.CENTER_ALIGNMENT);

        JPanel jButton = new JPanel();
        jButton.setBorder(new EmptyBorder(75, 0, 10, 0));

        JButton jbSend = new JButton("Send file");
        jbSend.setPreferredSize(new Dimension(150, 75));
        jbSend.setFont(new Font("Arial", Font.BOLD, 15));

        JButton jbChooseFile = new JButton("Choose file");
        jbChooseFile.setPreferredSize(new Dimension(150, 75));
        jbChooseFile.setFont(new Font("Arial", Font.BOLD, 15));

        jButton.add(jbSend);
        jButton.add(jbChooseFile);

        jbChooseFile.addActionListener( new ActionListener() {

            @Override
            public void actionPerformed(ActionEvent e) {
                JFileChooser fileChooser = new JFileChooser();
                fileChooser.setDialogTitle("Choose a file to send");

                if(fileChooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {
                    fileToSend[0] = fileChooser.getSelectedFile();
                    jFileName.setText("The file you want to send is: " + fileToSend[0].getName());
                }

            }
        });

        jbSend.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if(fileToSend[0] == null) {
                    jFileName.setText("Please choose a file first.");
                } else {
                    try {
                        FileInputStream fileInputStream = new  FileInputStream(fileToSend[0].getAbsolutePath());
                        Socket socket = new Socket(targetIP, port);

                        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());

                        String fileName = fileToSend[0].getName();
                        byte[] fileNameBytes = fileName.getBytes();

                        byte[] fileContentBytes = new byte[(int) fileToSend[0].length()];
                        fileInputStream.read(fileContentBytes);

                        dataOutputStream.writeInt(fileNameBytes.length);
                        dataOutputStream.write(fileNameBytes);

                        dataOutputStream.writeInt(fileContentBytes.length);
                        dataOutputStream.write(fileContentBytes);

                        dataOutputStream.flush();
                        dataOutputStream.close();
                        socket.close();
                        jFileName.setText("Sent: " + fileName);
                        jFrame.dispose();

                    } catch (IOException error) {
                        error.printStackTrace();
                    }
                }
            }
        });

        jFrame.add(jTitle);
        jFrame.add(jFileName);
        jFrame.add(jButton);
        jFrame.setVisible(true);
    }

    /**
     * This creates a new Frame for the sender, allowing them to choose a video file and stream it to the receiver.
     * The sender can choose any video file type, and the receiver will play it with the same name and extension.
     *
     *  @param targetIP IP to make bridge with
     *  @param port designated port for that bridge
     */
    private static void GUIStream(String targetIP, int port) {

        final File[] fileToSend = new File[1];

        JFrame jFrame = new JFrame("Video Stream");
        jFrame.setSize(450,450);
        jFrame.setLayout(new BoxLayout(jFrame.getContentPane(), BoxLayout.Y_AXIS));
        jFrame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

        JLabel jTitle = new JLabel("Stream Sender");
        jTitle.setFont(new Font("Arial", Font.BOLD, 25));
        jTitle.setBorder(new EmptyBorder(20, 0, 10, 0));
        jTitle.setAlignmentX(Component.CENTER_ALIGNMENT);

        JLabel jFileName = new JLabel("Choose a video to stream (.ts recommended).");
        jFileName.setFont(new Font("Arial", Font.BOLD, 20));
        jFileName.setBorder(new EmptyBorder(50, 0, 0, 0));
        jFileName.setAlignmentX(Component.CENTER_ALIGNMENT);

        JPanel jButton = new JPanel();
        jButton.setBorder(new EmptyBorder(75, 0, 10, 0));

        JButton jbSend = new JButton("Start stream");
        jbSend.setPreferredSize(new Dimension(150, 75));
        jbSend.setFont(new Font("Arial", Font.BOLD, 15));

        JButton jbChooseFile = new JButton("Choose file");
        jbChooseFile.setPreferredSize(new Dimension(150, 75));
        jbChooseFile.setFont(new Font("Arial", Font.BOLD, 15));

        jButton.add(jbSend);
        jButton.add(jbChooseFile);

        jbChooseFile.addActionListener(e -> {
            JFileChooser fileChooser = new JFileChooser();
            fileChooser.setDialogTitle("Choose a video to stream");
            if (fileChooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {
                fileToSend[0] = fileChooser.getSelectedFile();
                jFileName.setText("Video selected: " + fileToSend[0].getName());
            }
        });

        jbSend.addActionListener(e -> {
            if (fileToSend[0] == null) {
                jFileName.setText("Please choose a file first.");
                return;
            }

            new Thread(() -> {
                new StreamTransfer(targetIP, port, fileToSend[0]).send();
            }).start();

            jFrame.dispose();
        });

        jFrame.add(jTitle);
        jFrame.add(jFileName);
        jFrame.add(jButton);
        jFrame.setVisible(true);
    }
}
