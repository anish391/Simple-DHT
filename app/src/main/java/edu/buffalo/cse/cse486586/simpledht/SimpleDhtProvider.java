package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    private String myPort;
    private String myAvd;
    private String myHash;
    private String successorPort;
    private String predecessorPort;
    private String successorHash;
    private String predecessorHash;
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String HEAD_NODE = "5554";
    static final String HEAD_PORT = "11108";
    static final int SERVER_PORT = 10000;
    private static final String keyField = "key";
    private static final String valueField = "value";
    private ArrayList<Node> ring = new ArrayList<Node>();
    private ArrayList<String> keyList = new ArrayList<String>();
    private TreeMap<String, Integer> treeMap = new TreeMap();

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        myAvd = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(myAvd) * 2));
        Log.e(TAG,"AVD Number: " + myAvd);

        try{
            Log.v(TAG,"Server Socket creation");
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            myHash = genHash(myAvd);
            successorHash = genHash(myAvd);
            predecessorHash = genHash(myAvd);
            Log.v(TAG,"My AVD: "+myAvd);
            Log.v(TAG,"My Hash: "+myHash);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Node node = new Node(myHash,myPort);

        //If the node's ID does not match the head node(5554), then send join request to Head Node.
        if(!myAvd.equals(HEAD_NODE)){
            String message = "JOIN";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
        }
        //Add the node's hash and it's port to HashMap to maintain mapping.
        else{
            treeMap.put(myHash, Integer.parseInt(myPort));
            ring.add(node);
        }
        return false;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Log.e(TAG,"Delete function.");
        int count = 0;
        try {
            count = callDelete(selection);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }


    private int callDelete(String selection) throws IOException {
        int count = 0;
        //Single Node Delete. For single node, "*" and "@" are the same.
        if((selection.equals("*") || selection.equals("@")) && isOnly()){
            return singleNodeDelete();
        }
        // Local Delete All.
        else if(selection.equals("@")){
            Log.e(TAG,"Local Node Delete");
            return singleNodeDelete();
        }
        // Global Delete All.
        else if(selection.equals("*")){
            Log.e(TAG, "All Node Delete");
            return globalDelete(selection);
        }
        // Local Delete
        else if(keyList.contains(selection)){
            getContext().deleteFile(selection);
            keyList.remove(selection);
            count++;
            return count;
        }
        // If key does not exist in node, check successor.
        else{
            getContext().deleteFile(selection);
            count++;
            return count;
        }
    }

    private int singleNodeDelete(){
        int count =0;
        for(String key: keyList){
            getContext().deleteFile(key);
            count++;
        }
        return count;
    }

    private int globalDelete(String selection) throws IOException {
        int count;
        count = singleNodeDelete();
        count = count + sendGlobalDelete(selection);
        return count;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String key = (String) values.get(keyField);
        String value = (String) values.get(valueField)+"\n";
        try {
            callInsert(key,value);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void callInsert(String key, String value) throws IOException {

        String hashedMessage="";
        try {
            hashedMessage = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        // Single Node Insert.
        if(isOnly()){
            singleNodeInsert(key,value);
        }
        // Checking if key lies between two nodes from a list in a circular manner.
        else if(isBetween(hashedMessage) || isAfterLast(hashedMessage)){
            try {
                //Log.e(TAG,"Insert: "+successorPort);
                sendSuccessorMessage("INSERT",key,value,successorPort);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // If key does not lie in current node, query successor.
        else{
            //Log.e(TAG,"Succesor: "+successorPort);
            sendSuccessorMessage("SUCCESSOR",key,value,successorPort);
        }
    }

    // Checks if current node is the only node in ring.
    private boolean isOnly(){
        return (predecessorHash.equals(myHash) && successorHash.equals(myHash));
    }

    // Checks if node is between last and first node.
    private boolean isAfterLast(String key) {
        return (((key.compareTo(myHash) > 0 && key.compareTo(successorHash) > 0) ||
                (key.compareTo(myHash) < 0 && key.compareTo(successorHash) < 0)) &&
                myHash.compareTo(successorHash) > 0);
    }

    // Checks if node is between current node and successor node.
    private boolean isBetween(String key) {
        return (key.compareTo(myHash) > 0 &&
                key.compareTo(successorHash) < 0);
    }

    // Used to insert key, value pair in a given node.
    private void singleNodeInsert(String key, String value){
        try{
            keyList.add(key);
            FileOutputStream opStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            opStream.write(value.getBytes());
            opStream.close();
            System.out.println(keyList);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        MatrixCursor matrixCursor = null;
        try {
            matrixCursor = callQuery(selection);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return matrixCursor;
    }

    private MatrixCursor callQuery(String selection) throws IOException {
        Context context = getContext();
        String[] s = {"key","value"};
        MatrixCursor matrixCursor = new MatrixCursor(s);

        // Single Node Query. For single node, "*" and "@" are the same.
        if((selection.equals("*") || selection.equals("@")) && isOnly()){
            Log.e(TAG,"Single Node Query");
            return singleNodeQuery();
        }
        // Local Query All.
        else if(selection.equals("@")){
            Log.e(TAG,"Local Node Query");
            return singleNodeQuery();
        }
        // Global Query All.
        else if(selection.equals("*")){
            Log.e(TAG,"Global Query");
            return globalQuery();
        }
        // Local Query.
        else if(keyList.contains(selection)){
            Log.e(TAG,"Simple Node query.");
            try{
                InputStream inputStream = context.openFileInput(selection);
                if(inputStream !=null){
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                    String value = bufferedReader.readLine();
                    String[] pair = {selection,value};
                    matrixCursor.addRow(pair);
                    return matrixCursor;
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // If key does not lie in the node, query other nodes.
        else{
            Log.e(TAG,"Successor Node Query");
            try {
                String value ="";
                value = successorNodeQuery(selection, successorPort);
                Log.e(TAG,"Value: "+value);
                String[] pair = {selection,value};
                matrixCursor.addRow(pair);
                return matrixCursor;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    // Used to obtain all key value pairs in a single node.
    private MatrixCursor singleNodeQuery(){
        String[] s = {"key","value"};
        MatrixCursor matrixCursor = new MatrixCursor(s);
        for(String key:keyList){
            try{
                InputStream inputStream = getContext().openFileInput(key);
                if(inputStream !=null){
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                    String value = bufferedReader.readLine();
                    MatrixCursor.RowBuilder mRowBuilder = matrixCursor.newRow();
                    mRowBuilder.add(s[0], key);
                    mRowBuilder.add(s[1], value);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return matrixCursor;
    }

    // Used to find and return successor node if the key does not exist in current node.
    private String successorNodeQuery(String selection, String successor) throws IOException {
        String value = sendQueryMessage("QUERY", selection, successor);
        return value;
    }

    // Used to query all nodes and return all key, value pairs.
    private MatrixCursor globalQuery() throws IOException {
        TreeMap<String, String> global = new TreeMap<String, String>();
        String[] s = {"key","value"};
        MatrixCursor matrixCursor = new MatrixCursor(s);
        for(String k:keyList){
            InputStream inputStream = getContext().openFileInput(k);
            if(inputStream !=null) {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                String value = bufferedReader.readLine();
                MatrixCursor.RowBuilder mRowBuilder = matrixCursor.newRow();
                mRowBuilder.add(s[0], k);
                mRowBuilder.add(s[1], value);
            }
        }
        global = sendGlobalMessage("GLOBAL",successorPort);
        Log.e(TAG,"GlobalQuery size: "+global.size());
        System.out.println(global);
        for(String k: global.keySet()){
            MatrixCursor.RowBuilder mRowBuilder = matrixCursor.newRow();
            Log.e(TAG,"GLOBAL KEY: "+k);
            Log.e(TAG,"GLOBAL VALUE: "+global.get(k));
            mRowBuilder.add(s[0], k);
            mRowBuilder.add(s[1], global.get(k));
        }
        return matrixCursor;
    }
    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }



    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            ServerSocket serverSocket = serverSockets[0];
            try{
                while(true){
                    Socket clientSocket = serverSocket.accept();
                    DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
                    DataInputStream in = new DataInputStream(clientSocket.getInputStream());
                    String ip;
                    while((ip = in.readUTF())!=null){
                        String clientContent[] = ip.split("<><");
                        // Only received at 5554. Stores nodes and finds Predecessor & Successor
                        if(clientContent[0].equals("JOIN")){
                            int port = Integer.parseInt(clientContent[1]);
                            String n = Integer.toString(port/2);
                            String hashedNode = genHash(Integer.toString(port/2));
                            treeMap.put(hashedNode, port);
                            Node node = new Node(hashedNode, clientContent[1]);
                            ring.add(node);
                            findPredecessorSuccessor();
                            out.writeUTF("ACK");
                            out.flush();
                            break;
                        }
                        // Store values of Predecessor and Successor Hashes and Ports.
                        if(clientContent[0].equals("PREDSUCC")){
                            predecessorHash = clientContent[1];
                            successorHash = clientContent[2];
                            predecessorPort = clientContent[3];
                            successorPort = clientContent[4];
                            out.writeUTF("ACK");
                            out.flush();
                            break;
                        }
                        // Receive key, value pair to check in current AVD.
                        if(clientContent[0].equals("SUCCESSOR")){
                            String key = clientContent[1];
                            String value = clientContent[2];
                            String port = clientContent[3];
                            callInsert(key, value);
                            out.writeUTF("ACK");
                            out.flush();
                            break;
                        }
                        // Insert in current AVD.
                        if(clientContent[0].equals("INSERT")){
                            String key = clientContent[1];
                            String value = clientContent[2];
                            String port = clientContent[3];
                            singleNodeInsert(key,value);
                            out.writeUTF("ACK");
                            out.flush();
                            break;
                        }
                        // Query key in current AVD. If doesn't exist then send Successor Port.
                        if(clientContent[0].equals("QUERY")){
                            String key = clientContent[1];
                            String value="";
                            if(keyList.contains(key)){
                                Log.e(TAG,myPort+" contains key:"+key);
                                InputStream inputStream = getContext().openFileInput(key);
                                if(inputStream !=null) {
                                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                                    value = bufferedReader.readLine();
                                    Log.e(TAG, "Value: " + value);
                                    out.writeUTF("YES" + "<><" + value);
                                    out.flush();
                                    break;
                                }
                            }
                            else{
                                Log.e(TAG,myPort+" does not contain key "+key);
                                out.writeUTF("NO"+"<><"+successorPort);
                                out.flush();
                                break;
                            }
                        }
                        // Return Successor port & key, value pairs as a concatenated string.
                        if(clientContent[0].equals("GLOBAL")){
                            String send =successorPort;
                            for(String key: keyList){
                                InputStream inputStream = getContext().openFileInput(key);
                                if(inputStream !=null) {
                                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                                    String value = bufferedReader.readLine();
                                    Log.e(TAG, "Value: " + value);
                                    send = send+"<><"+key+"<><"+value;
                                }
                            }
                            out.writeUTF(send);
                            out.flush();
                            break;
                        }
                        // Delete all files in current AVD and return Successor Port.
                        if(clientContent[0].equals("DELETE")){
                            String send =successorPort;
                            int count = 0;
                            for(String key: keyList){
                                getContext().deleteFile(key);
                                count+=1;
                            }
                            send = send+Integer.toString(count);
                            out.writeUTF(send);
                            out.flush();
                            break;
                        }
                    }
                    out.close();
                    in.close();
                    clientSocket.close();

                    // Continuously update predecessor and successor of all ports.
                    if(myPort.equals(HEAD_PORT) && ring.size()>1){
                        for(Node n:ring){
                            if(Integer.toString(treeMap.get(n.myHash)).equals(HEAD_PORT)){
                                predecessorHash = n.myPredecessor;
                                successorHash = n.mySuccessor;
                                predecessorPort = Integer.toString(treeMap.get(n.myPredecessor));
                                successorPort = Integer.toString(treeMap.get(n.mySuccessor));
                                continue;
                            }
                            sendUpdateMessage("PREDSUCC",
                                    Integer.toString(treeMap.get(n.myHash)),
                                    n.myPredecessor, n.mySuccessor,
                                    Integer.toString(treeMap.get(n.myPredecessor)),
                                    Integer.toString(treeMap.get(n.mySuccessor)));
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                Log.e(TAG,"Server error.");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    // Finds Predecessor and Successor for a given node.
    private void findPredecessorSuccessor() throws IOException {
        Collections.sort(ring);
        String pred="", succ="";
        int n = ring.size();
        for (int i = 0; i < n; i++) {
            Node node = ring.get(i);
            node.myPredecessor = ring.get((n+i-1)%n).myHash;
            node.mySuccessor = ring.get((n+i+1)%n).myHash;
            ring.set(i, node);
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void>{

        @Override
        protected Void doInBackground(String... strings) {
            String[] string = strings[0].split("<><");

            // Sends JOIN message to Head Node(5554)
            if(string[0].equals("JOIN")){
                try {
                    sendJoinMessage("JOIN", HEAD_PORT);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }

    // Sends JOIN message along with current node's port number to Head Node.
    private void sendJoinMessage(String type, String port) throws IOException {
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                Integer.parseInt(port));
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(socket.getInputStream());
        out.writeUTF("JOIN"+"<><"+myPort);
        out.flush();
        while(!socket.isClosed()){
            String ack = in.readUTF();
            if(ack!=null){
                out.close();
                socket.close();
            }
        }
        out.close();
        in.close();
        socket.close();
    }

    // Sends Predecessor and Successor Port and Hash to a given node.
    private void sendUpdateMessage(String type, String port, String predecessorHash, String successorHash, String predecessor, String successor) throws IOException {
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                Integer.parseInt(port));
        //Log.e(TAG,"Port to send: "+port);
        //Log.e(TAG,"Update Message");
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(socket.getInputStream());
        out.writeUTF("PREDSUCC"+"<><"+predecessorHash+"<><"+successorHash+"<><"+predecessor+"<><"+successor);
        out.flush();
        while(!socket.isClosed()){
            String ack = in.readUTF();
            if(ack!=null){
                out.close();
                socket.close();
            }
        }
        out.close();
        in.close();
        socket.close();
    }

    // Forwards the current key, value pair to Successor port.
    private void sendSuccessorMessage(String type, String key, String value, String port) throws IOException {
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                Integer.parseInt(port));
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(socket.getInputStream());
        // type = "SUCCESSOR"
        out.writeUTF(type+"<><"+key+"<><"+value+"<><"+port);
        out.flush();
        while(!socket.isClosed()){
            String ack = in.readUTF();
            if(ack!=null){
                out.close();
                socket.close();
            }
        }
        out.close();
        in.close();
        socket.close();
    }

    // Forwards current key to successor port to find and obtain corresponding value.
    private String sendQueryMessage(String type, String key, String port) throws IOException {
        String value = "";
        while(!port.equals(myPort)){
            Log.e(TAG,"My Port: "+myPort);
            Log.e(TAG,"Successor Port:"+port);
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port));
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());
            // type = "QUERY"
            out.writeUTF(type+"<><"+key+"<><"+port);
            out.flush();
            while(!socket.isClosed()){
                String ack = in.readUTF();
                String[] serverContent = ack.split("<><");
                if(serverContent[0].equals("YES")){
                    value = serverContent[1];
                    Log.e(TAG,"Found value at Port: "+port);
                    Log.e(TAG,"Value: "+value);
                    port = myPort;
                    out.close();
                    socket.close();
                    break;
                }
                else if(serverContent[0].equals("NO")){
                    port = serverContent[1];
                    out.close();
                    socket.close();
                }
            }
            out.close();
            in.close();
            socket.close();
        }
        Log.e(TAG,"While loop breaks. "+value);
        return value;
    }

    // Gets a String of all key, value pairs from all nodes by doing String Manipulation.
    // Adds the key,value pairs to a TreeMap and returns to the caller.
    private TreeMap<String, String> sendGlobalMessage(String type, String port) throws IOException {
        TreeMap<String, String> global = new TreeMap<String, String>();
        String ogPort = port;
        while(!port.equals(myPort)){
            Log.e(TAG,"Original Port:"+ogPort);
            Log.e(TAG,"Successor Port from function:"+port);
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port));
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());
            out.writeUTF("GLOBAL");
            out.flush();
            while(!socket.isClosed()){
                String ack = in.readUTF();
                String[] serverContent = ack.split("<><");
                Log.e(TAG,serverContent[0]);
                if(ack!=null){
                    port = serverContent[0];
                    Log.e(TAG,"Size of Key Value"+serverContent.length);
                    System.out.println(ack);
                    for(int i=1;i<serverContent.length;i+=2){
                        global.put(serverContent[i],serverContent[i+1]);
                    }
                    out.close();
                    socket.close();
                    break;
                }
            }
            out.close();
            in.close();
            socket.close();
        }
        return global;
    }

    // Connects to all nodes and deletes the key, value pairs present in them.
    private int sendGlobalDelete(String port) throws IOException {
        int count =0;
        String ogPort = port;
        while(!port.equals(myPort)){
            Log.e(TAG,"Original Port:"+ogPort);
            Log.e(TAG,"Successor Port from function:"+port);
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port));
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());
            out.writeUTF("DELETE");
            out.flush();
            while(!socket.isClosed()){
                String ack = in.readUTF();
                String[] serverContent = ack.split("<><");
                Log.e(TAG,serverContent[0]);
                if(ack!=null){
                    port = serverContent[0];
                    count = count + Integer.parseInt(serverContent[1]);
                    Log.e(TAG,"Size of DELETE Value"+serverContent.length);
                    System.out.println(ack);
                    out.close();
                    socket.close();
                    break;
                }
            }
            out.close();
            in.close();
            socket.close();
        }
        return count;
    }
}
