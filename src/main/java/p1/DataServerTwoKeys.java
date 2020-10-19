package p1;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class DataServerTwoKeys {
    public static void main(String[] args) throws IOException, InterruptedException{
        try (ServerSocket listener = new ServerSocket(9090)) {

            Socket socket = listener.accept();
            PrintWriter out  = new PrintWriter(socket.getOutputStream(), true);

            for (int i = 0; i < 50; i++){
                int key = i % 2 + 1;
                String row = key + "," + i;
                out.println(row);
                Thread.sleep(50);
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }
}
