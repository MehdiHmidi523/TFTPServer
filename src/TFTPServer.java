import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**  TFTP (Trivial File Transfer Protocol) is UDP (User Datagram Protocol) based **/
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
    public static final int ACCEPT_TIMEOUT_MILLIS = 200;
    private boolean receiver = true;
    private DatagramPacket fragmentOfData;

    //x
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

    //x
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
                    System.out.printf("%s request for %s from %s using port %d\n", (reqtype == OP_RRQ) ? "Read" : "Write", requestedFile, clientAddress.getHostName(), clientAddress.getPort());
                    if (reqtype == OP_RRQ) {
                        requestedFile.insert(0, READ_DIR);
                        HandleRQ(sendSocket, requestedFile.toString(), OP_RRQ);
                    } else if (reqtype == OP_WRQ) {
                        requestedFile.insert(0, WRITE_DIR);
                        HandleRQ(sendSocket, requestedFile.toString(), OP_WRQ);
                    } else if (reqtype == -99)
                        TransmitERROR(sendSocket, 0, "--- OCTET MODE ONLY ---");
                    else
                        TransmitERROR(sendSocket, 4, "Illegal TFTP Operation");
                    sendSocket.close();
                } catch (SocketException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }

    //x
    private InetSocketAddress receiveFrom(DatagramSocket socket, byte[] buf) {
        DatagramPacket receivedPack = new DatagramPacket(buf, buf.length);
        try {
            socket.receive(receivedPack);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new InetSocketAddress(receivedPack.getAddress(), receivedPack.getPort());
    }

    //x
    private int ParseRQ(byte[] buf, StringBuffer requestedFile) {
        try {
            int nameLength = 1;
            try { // loop stops at |0| Specification is:  |opcode|filename|0|
                do {
                    nameLength++;
                } while (buf[nameLength] != 0);
            } catch (IndexOutOfBoundsException io) {
                return -99;
            }

            requestedFile.append(new String(buf, 2, nameLength - 2));
            System.out.printf("FILE: %s \n", requestedFile.toString());

            StringBuffer transfer = new StringBuffer();
            transfer.append(new String(buf, nameLength + 1, 5));

            System.out.printf("MODE: %s \n", transfer);
            if (!transfer.toString().equals("octet"))
                throw new IllegalArgumentException("-- TRANSFER FORM NOT OCTET --");

            // Reads the next two bytes at this buffer's current position
            short opcode = ByteBuffer.wrap(buf).getShort();
            System.out.printf("OPCODE:	%d \n", opcode);
            return opcode;
        } catch (IllegalArgumentException e) {
            return -99;
        }
    }

    //x
    private void HandleRQ(DatagramSocket sendSocket, String dataRQ, int opcode) throws IOException {
        if (opcode == OP_RRQ) {
            try {
                readRequest(sendSocket, dataRQ);
            } catch (FileNotFoundException e) {
                TransmitERROR(sendSocket, 1, "File not found");
            } catch (IOException ex) {
                TransmitERROR(sendSocket, 0, "File could not be read");
            }
        } else if (opcode == OP_WRQ) {
            try {
                writeRequest(sendSocket, dataRQ);
            } catch (FileNotFoundException e) {
                TransmitERROR(sendSocket, 1, "File not found");
            } catch (IOException e) {
                TransmitERROR(sendSocket, 2, "Access violation");
            }
        }
    }

    //x
    private boolean databaseTier(DatagramPacket fragmentOfData) {
        File repo = new File(WRITE_DIR);
        long StorageFull = 0;
        for (File files : repo.listFiles())
            StorageFull += files.length();   // Current Server storage size.
        if (fragmentOfData.getLength()> 10000000 - StorageFull  ) {
            System.err.println("----- CRITICAL ERROR: DISK FULL -----");
            return true;
        }
        System.out.println("<<<< File received");
        return false;
    }

    //x
    private void writeRequest(DatagramSocket sendSocket, String dataRQ) throws FileNotFoundException, IOException {
        File createFile = new File(dataRQ);
        if (createFile.exists() && !createFile.isDirectory()) {
            TransmitERROR(sendSocket, 6, "File already exists");
            return;
        }

        // ERSTE ACK
        ByteBuffer ACKpacket = ByteBuffer.allocate(4);
        ACKpacket.putShort((short) OP_ACK);
        ACKpacket.putShort((short) 0);// Initiate correspondance
        sendSocket.send(new DatagramPacket(ACKpacket.array(), ACKpacket.position()));
        FileOutputStream stream = new FileOutputStream(createFile);
        int transmit = 0;
        int i = 0; // Packet ID
        while (receiver) {
            if (!receive_DATA_send_ACK(sendSocket, createFile, i + 1)) {
                if (transmit > 4) {
                    TransmitERROR(sendSocket, 0, "------ 5 Unsuccessful re-transmissions! TransmitERROR ------");
                    break;
                }
                i--;
                transmit++;
            }
            i++;
            transmit = 0;
            stream.write(fragmentOfData.getData(), 4, fragmentOfData.getLength() - 4);
            /** ---- >>> Server Action: Write to repo ---- **/
            //Writes len bytes from the specified byte array starting at offset off to this file output stream.
            //End >> LAST PACKET
            if (fragmentOfData.getLength() < MTU + 4){
                receiver = false;
                if(databaseTier(fragmentOfData))
                    TransmitERROR(sendSocket, 3, "Allocation exceeds Server Storage.");
            }
        }
    }

    //x
    private void readRequest(DatagramSocket sendSocket, String dataRQ) throws FileNotFoundException, IOException {
        File storage = new File(dataRQ);
        if (!storage.getParentFile().getName().equals("read")) { // Avoid request attempts to change directory with path
            TransmitERROR(sendSocket, 2, "Access violation");
            return;
        }
        // If file doesn't exist in the read dir then throw FileNotFoundException
        FileInputStream dataStream = new FileInputStream(storage);

        //Interested in the side effect
        //Reads some number of bytes from the input stream and stores them into the buffer array b.
        byte[] buffer = new byte[(int) storage.length()];
        dataStream.read(buffer);

        // Attempt at least one block |512 DATA|2 OPCODE |2 BLOCK Nr|
        int listOfBlocks = (int) storage.length() / MTU + 1;
        int transmit = 0; // Maximum 5 retransmissions

        //Move WND Edge forward by multiplying with the MTU
        int LEFT_EDGE = 0;
        while (LEFT_EDGE < listOfBlocks) {

            if (LEFT_EDGE + 1 == listOfBlocks) {
                byte[] lastWin = Arrays.copyOfRange(buffer, LEFT_EDGE * MTU, (int) storage.length());
                if (!send_DATA_receive_ACK(sendSocket, LEFT_EDGE + 1, lastWin)) {
                    if (transmit > 4) {
                        TransmitERROR(sendSocket, 0, "------ 5 Unsuccessful re-transmissions! TransmitERROR ------");
                        break;
                    }
                    transmit++;
                    LEFT_EDGE--;
                    continue;
                }
            } else {
                byte[] win = Arrays.copyOfRange(buffer, LEFT_EDGE * MTU, (LEFT_EDGE + 1) * MTU);
                if (!send_DATA_receive_ACK(sendSocket, LEFT_EDGE + 1, win)) {
                    if (transmit > 4) {
                        TransmitERROR(sendSocket, 0, "------ 5 Unsuccessful re-transmissions! TransmitERROR ------");
                        break;
                    }
                    transmit++;
                    LEFT_EDGE--;
                    continue;
                }
            }
            transmit = 0; // restart counter if transmission successful
            LEFT_EDGE++;
        }
        dataStream.close(); // Successful operation, cleanUp
    }

    //  x          DATA_ packet
    // |  2bytes  |	2bytes  |  n bytes |
    // |----------|---------|----------|
    // |  OpCode  |  Block# |   Data   |
    private boolean send_DATA_receive_ACK(DatagramSocket sendSocket, int i, byte[] body) throws IOException {
        try {
            /** Send DATA to Client **/
            ByteBuffer packet = ByteBuffer.allocate(body.length + 4);
            packet.putShort((short) OP_DAT);
            packet.putShort((short) i);
            packet.put(body);
            sendSocket.send(new DatagramPacket(packet.array(), packet.position()));
            System.out.println(">>> "+i+" DATA_Sent");

            /** receive ACK from Client **/
            byte[] RQ = new byte[4];
            sendSocket.setSoTimeout(ACCEPT_TIMEOUT_MILLIS);
            sendSocket.receive(new DatagramPacket(RQ, 4));
            System.out.println("<<<  ACK"); // ACK PACKET SIZE = 4

            /** Check Client status **/
            ByteBuffer wrap = ByteBuffer.wrap(RQ);
            short opCode = wrap.getShort();
            if (opCode != OP_ACK && opCode != OP_ERR){
                TransmitERROR(sendSocket, 4, "Illegal TFTP operation.");
                return false;
            }
            return (wrap.getShort() == (short) i);
        } catch (SocketTimeoutException e) {
            System.err.println("----- CLIENT TIME OUT -----");
            return false;
        } catch (IOException e) {
            TransmitERROR(sendSocket, 2, "Access violation.");
            return false;
        }
    }

    //  x     ACK_ packet
    // | 2bytes   |	2bytes  |
    // |----------|---------|
    // |  OpCode  |  Block# |
    private boolean receive_DATA_send_ACK(DatagramSocket sendSocket, File createFile, int i) throws IOException {
        try {
            /** ------ receive_DATA ----- **/
            byte[] data = new byte[BUFFER_SIZE];
            fragmentOfData = new DatagramPacket(data, BUFFER_SIZE);
            sendSocket.setSoTimeout(ACCEPT_TIMEOUT_MILLIS);
            sendSocket.receive(fragmentOfData);
            System.out.println("<<< PACKET SIZE: " + fragmentOfData.getLength());

            /** ------ Check Status ----- **/
            boolean correct = true;
            // Reaction expected:  Port changes >>> Disconnect and send Error message to new port.
            if (fragmentOfData.getPort() != sendSocket.getPort()) {
                sendSocket.disconnect();
                sendSocket.connect(new InetSocketAddress(sendSocket.getInetAddress(), fragmentOfData.getPort()));
                TransmitERROR(sendSocket, 5, "Unknown transfer ID");
                correct = false;
            }

            ByteBuffer wrap = ByteBuffer.wrap(fragmentOfData.getData());

            // illegal operation
            short opCode = wrap.getShort();
            if (opCode != OP_ERR && opCode != OP_DAT) {
                TransmitERROR(sendSocket, 4, "Illegal TFTP operation.");
                System.err.println("-- UNKNOWN OP CODE° --");
                throw new SocketException();
            }

            //  Packet ID <> block Nr of ACK
            short n = wrap.getShort();
            if (i != n) {
                TransmitERROR(sendSocket, 5, "Unknown transfer ID");
                System.err.println(" PACKET Nr° <</>> ACK BLOCK Nr°");
                throw new SocketException();
            }

            /** ------ Send ACK ----- **/
            if (correct) {
                ByteBuffer ACKpacket = ByteBuffer.allocate(4);
                ACKpacket.putShort((short) OP_ACK);
                ACKpacket.putShort((short) n);
                sendSocket.send(new DatagramPacket(ACKpacket.array(), ACKpacket.position()));
                System.out.println(">>> ACK Nr°: " + n);
            }
            return true;
        } catch (SocketTimeoutException e) {
            System.err.println("--- CLIENT TIME OUT ---");
            return false;
        } catch (SocketException x) {
            System.err.println("--- CLIENT PACKET HEADER ERROR ---");
            return false;
        } catch (IOException e) {
            TransmitERROR(sendSocket, 2, "Access violation.");
            return false;
        }
    }

    //  x        ERR_ packet
    // |2 bytes|  2 bytes   |   n bytes  |  1 byte |
    // |-------|------------|------------|---------|
    // |   06  |  ErrorCode |   ErrMsg   |     0   |
    private void TransmitERROR(DatagramSocket sendSocket, int nr, String alert) {
        byte[] buf = alert.getBytes();
        ByteBuffer ERRpacket = ByteBuffer.allocate(alert.getBytes().length + 4);
        ERRpacket.putShort((short) OP_ERR);
        ERRpacket.putShort((short) nr);
        ERRpacket.put(buf);
        try {
            sendSocket.send(new DatagramPacket(ERRpacket.array(), ERRpacket.position()));
        } catch (IOException e) {
            e.getMessage();
        }
    }
}
