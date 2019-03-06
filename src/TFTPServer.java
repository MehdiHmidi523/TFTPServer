import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class TFTPServer {
    public static final int ACCEPT_PORT = 4970;
    public static final int BUFFER_SIZE = 516;
    public static final String READ_DIR = "src/repo/read/";
    public static final String WRITE_DIR = "src/repo/write/";
    public static final int OP_RRQ = 1;
    public static final int OP_WRQ = 2;
    public static final int OP_DAT = 3;
    public static final int OP_ACK = 4;
    public static final int OP_ERR = 5;
    public static final int MTU = 512;
    public static final int ACK_PACKET_SIZE = 4;
    public static final int ACCEPT_TIMEOUT_MILLIS = 200;
    private DatagramPacket fragmentOfData;

    public static void main(String[] args) {
        if (args.length > 0) {
            System.err.printf("usage: java %s\n", TFTPServer.class.getCanonicalName());
            System.exit(1);
        }
        try {
            TFTPServer server = new TFTPServer();
            server.start();
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }

    private void start() throws SocketException {
        byte[] buf = new byte[BUFFER_SIZE];
        DatagramSocket socket = new DatagramSocket(null);
        SocketAddress localBindPoint = new InetSocketAddress(ACCEPT_PORT);
        socket.bind(localBindPoint);
        System.out.printf("Listening at port %d for new requests\n", ACCEPT_PORT);
        while (true) {
            final InetSocketAddress clientAddress = receiveFrom(socket, buf);
            if (clientAddress == null)
                continue;
            final StringBuffer requestedFile = new StringBuffer();
            final int reqtype = ParseRQ(buf, requestedFile);
            new Thread(() -> {
                try {
                    DatagramSocket sendSocket = new DatagramSocket(0);
                    sendSocket.connect(clientAddress);
                    System.out.printf("%s request for %s from %s using port %d\n",
                            (reqtype == OP_RRQ) ? "Read" : "Write",
                            requestedFile, clientAddress.getHostName(), clientAddress.getPort());
                    if (reqtype == OP_RRQ) {
                        requestedFile.insert(0, READ_DIR);
                        HandleRQ(sendSocket, requestedFile.toString(), OP_RRQ);
                    } else if (reqtype == OP_WRQ) {
                        requestedFile.insert(0, WRITE_DIR);
                        HandleRQ(sendSocket, requestedFile.toString(), OP_WRQ);
                    } else
                        TransmitERROR(sendSocket, 4, "Illegal TFTP Operation.");
                    sendSocket.close();
                } catch (SocketException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }

    private InetSocketAddress receiveFrom(DatagramSocket socket, byte[] buf) {
        DatagramPacket receivedPack = new DatagramPacket(buf, buf.length);
        try {
            socket.receive(receivedPack);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new InetSocketAddress(receivedPack.getAddress(), receivedPack.getPort());
    }

    private int ParseRQ(byte[] buf, StringBuffer requestedFile) {
        ByteBuffer buffer = ByteBuffer.wrap(buf); // for fast low-level I/O
        int packetEnd = 0;//  Following Specification |opcode|filename|0| loop stops at |0|
        for (int i = 1; i < buf.length; i++)
            if (buf[i] == 0) {
                packetEnd = i;
                break;
            }
        StringBuffer mode = new StringBuffer();
        mode.append(new String(buf, packetEnd + 1, 5));
        System.out.println("MODE: " + mode);
        if (!mode.toString().equals("octet"))
            throw new IllegalArgumentException("TRANSFER FORM NOT PUT INTO OPERATION");
        int opcode = buffer.getShort();
        System.out.printf("OPCODE:	%d", opcode);
        requestedFile.append(new String(buf, 2, packetEnd - 2));
        System.out.printf("FILE: %s", requestedFile.toString());
        return opcode;
    }

    private void HandleRQ(DatagramSocket sendSocket, String dataRQ, int opcode) throws IOException {
        if (opcode == OP_RRQ) {
            try {
                File storage = new File(dataRQ);
                if (!storage.getParentFile().getName().equals("read")) {
                    TransmitERROR(sendSocket, 2, "Access violation");
                    return;
                }
                // File exists and in read repo
                // Problem covered in catch unit.
                FileInputStream dataStream = new FileInputStream(storage);

                //Interested in the side effect
                //Reads some number of bytes from the input stream and stores them into the buffer array b.
                byte[] buffer = new byte[(int) storage.length()];
                dataStream.read(buffer);

                // Attempt at least one block |512 DATA|2 OPCODE |2 BLOCK Nr|
                int listOfBlocks = (int) storage.length() / MTU + 1;
                int transmit = 0; // Maximum Retransmission is 5

                //Move WND Edge forward by multiplying with the MTU
                int LEFT_EDGE = 0;
                while (LEFT_EDGE < listOfBlocks) {
                    if (LEFT_EDGE + 1 == listOfBlocks) {
                        byte[] lastWin = Arrays.copyOfRange(buffer, LEFT_EDGE * MTU, (int) storage.length());
                        if (!send_DATA_receive_ACK(sendSocket, LEFT_EDGE + 1, lastWin)) {
                            if (transmit > 4) {
                                TransmitERROR(sendSocket, 0, "------ Unsuccessful re-transmissions! TransmitERROR ------");
                                break;
                            }
                            transmit++;
                            LEFT_EDGE--;
                        }
                        transmit = 0;
                    } else { // last transmission
                        byte[] win = Arrays.copyOfRange(buffer, LEFT_EDGE * MTU, (LEFT_EDGE + 1) * MTU);
                        if (!send_DATA_receive_ACK(sendSocket, LEFT_EDGE + 1, win)) {
                            if (transmit > 4) {
                                TransmitERROR(sendSocket, 0, "------ Unsuccessful re-transmissions! TransmitERROR ------");
                                break;
                            }
                            transmit++;
                            LEFT_EDGE--;
                        }
                        transmit = 0; // restart counter if transmission successful
                    }
                    LEFT_EDGE++;
                }
                dataStream.close(); // Successful operation, cleanUp
            } catch (FileNotFoundException e) {
                TransmitERROR(sendSocket, 1, "File not found");
            } catch (IOException ex) {
                TransmitERROR(sendSocket, 0, "File could not be read");
            }
        } else if (opcode == OP_WRQ) {
            try {
                File createFile = new File(dataRQ);
                if (createFile.exists() && !createFile.isDirectory()) {
                    TransmitERROR(sendSocket, 6, "File already exists");
                    return;
                }
                FileOutputStream stream = new FileOutputStream(createFile);

                // Initiate correspondance
                ByteBuffer ACKpacket = ByteBuffer.allocate(4);
                ACKpacket.putShort((short) OP_ACK);
                ACKpacket.putShort((short) 0);
                sendSocket.send(new DatagramPacket(ACKpacket.array(), ACKpacket.position()));

                int transmit = 0;
                int i = 0;
                while (true) {
                    if (!receive_DATA_send_ACK(sendSocket, i + 1)) {
                        if (transmit > 4) {
                            TransmitERROR(sendSocket, 0, "------ Unsuccessful re-transmissions! TransmitERROR ------");
                            break;
                        }
                        i--;
                        transmit++;
                    }
                    i++;

                    stream.write(fragmentOfData.getData(), 4, fragmentOfData.getLength() - 4);
                    if (fragmentOfData.getLength() < MTU + 4) {
                        stream.close();
                        break;
                    }
                    transmit = 0;
                }

                File repo = new File(WRITE_DIR);
                long StorageFull = 0;
                long ServerStorage = 10000000;   // Server persistance storage size.
                for (File files : repo.listFiles())
                    StorageFull += files.length();   // Current Server storage size.
                if (ServerStorage - StorageFull < fragmentOfData.getLength()) {
                    System.err.println("DISK FULL");
                    TransmitERROR(sendSocket, 3, "Allocation exceeds Server Storage.");
                    return;
                }
                System.out.println("<<<< File received");
            } catch (FileNotFoundException e) {
                TransmitERROR(sendSocket, 1, "File not found");
            } catch (IOException e) {
                TransmitERROR(sendSocket, 2, "Access violation");
            }
        }
    }

    //            DATA_ packet
    // |  2bytes  |	2bytes  |  n bytes |
    // |----------|---------|----------|
    // |  OpCode  |  Block# |   Data   |
    private boolean send_DATA_receive_ACK(DatagramSocket sendSocket, int i, byte[] body) throws IOException {
        try {
            ByteBuffer packet = ByteBuffer.allocate(body.length + 4);
            packet.putShort((short) OP_DAT);
            packet.putShort((short) i);
            packet.put(body);

            sendSocket.send(new DatagramPacket(packet.array(), packet.position()));
            System.out.println(">>>  Sent");

            byte[] recACK = new byte[ACK_PACKET_SIZE];
            DatagramPacket receivedACKPacket = new DatagramPacket(recACK, ACK_PACKET_SIZE);
            sendSocket.setSoTimeout(ACCEPT_TIMEOUT_MILLIS);
            sendSocket.receive(receivedACKPacket);
            System.out.println("<<<  ACK");

            ByteBuffer wrap = ByteBuffer.wrap(recACK);
            short opCode = wrap.getShort();
            short blockNr = wrap.getShort();

            if (opCode != OP_ACK && opCode != OP_ERR)
                TransmitERROR(sendSocket, 4, "Illegal TFTP operation.");
            else if (blockNr != (short) i)
                return false;

        } catch (SocketTimeoutException e) {
            System.err.println("----- TIME OUT -----");
        } catch (IOException e) {
            TransmitERROR(sendSocket, 2, "Access violation.");
        }
        return true;
    }

    //       ACK_ packet
    // | 2bytes   |	2bytes  |
    // |----------|---------|
    // |  OpCode  |  Block# |
    private boolean receive_DATA_send_ACK(DatagramSocket sendSocket, int i) throws IOException {
        try {
            /** ------ receive_DATA ----- **/
            byte[] data = new byte[BUFFER_SIZE];
            fragmentOfData = new DatagramPacket(data, BUFFER_SIZE);
            sendSocket.setSoTimeout(ACCEPT_TIMEOUT_MILLIS);
            sendSocket.receive(fragmentOfData);
            System.out.print("<<< PACKET SIZE: " + fragmentOfData.getLength());

            /** ------ Check Status ----- **/
            boolean correct = true;
            // Reaction expected:  Port changes >>> Disconnect and send Error message to new port.
            if (fragmentOfData.getPort() != sendSocket.getPort()) {
                sendSocket.disconnect();
                sendSocket.connect(new InetSocketAddress(sendSocket.getInetAddress(), fragmentOfData.getPort()));
                TransmitERROR(sendSocket, 5, "Unknown transfer ID");
                correct=false;
            }

            byte[] header = fragmentOfData.getData();
            ByteBuffer wrap = ByteBuffer.wrap(header);

            // illegal operation
            int opCode = wrap.getShort();
            if (opCode != OP_ERR && opCode != OP_DAT) {
                TransmitERROR(sendSocket, 4, "Illegal TFTP operation.");
                correct=false;
                throw new SocketException();
            }

            //  Packet ID <> blockNr of ACK
            int blockNr = wrap.getShort();
            if (blockNr != i) {
                TransmitERROR(sendSocket, 5, "Unknown transfer ID");
                System.out.println(" PACKET Nr° <</>> ACK BLOCK Nr°");
                correct=false;
                throw new SocketException();
            }

            /** ------ Send ACK ----- **/
            if(correct){
                ByteBuffer ACKpacket = ByteBuffer.allocate(4);
                ACKpacket.putShort((short) OP_ACK);
                ACKpacket.putShort((short) blockNr);
                sendSocket.send(new DatagramPacket(ACKpacket.array(), ACKpacket.position()));
                System.out.println(">>> ACK Nr°: " + blockNr);
            }

        } catch (SocketException e) {
            System.err.println("--- TIMEOUT ---");
        } catch (IOException e) {
            TransmitERROR(sendSocket, 2, "Access violation.");
        }
        return true;
    }

    //          ERR_ packet
    // |2 bytes|  2 bytes   |   n bytes  |  1 byte |
    // |-------|------------|------------|---------|
    // |   06  |  ErrorCode |   ErrMsg   |     0   |
    private void TransmitERROR(DatagramSocket sendSocket, int nr, String alert) throws IOException {
        byte[] buf = alert.getBytes(); //send error to client in bytes.
        ByteBuffer ERRpacket = ByteBuffer.allocate(alert.getBytes().length + 4);
        ERRpacket.putShort((short) OP_ERR);
        ERRpacket.putShort((short) nr);
        ERRpacket.put(buf);
        sendSocket.send(new DatagramPacket(ERRpacket.array(), ERRpacket.position()));
    }
}
