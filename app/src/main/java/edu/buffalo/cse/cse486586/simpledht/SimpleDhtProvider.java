package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

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

    private static final String TAG = SimpleDhtProvider.class.getSimpleName();

    private static final int SERVER_PORT = 10000;
    private static final String AVD_JOIN_REQ = "11108";

    private static Set<String> chord = new HashSet<String>();
    private static String[] cols = new String[] { "key", "value" };
    private static String query_global = "";
    private static MatrixCursor cursor = null;
    private static boolean myQuery = false;
    private static Message node = null;
    private static boolean isDeleted = false;
    private static String checkData = null;
    Message predecessor_node = new Message();
    Message successor_node = new Message();

    private static final String GLOBAL_STAR_QUERY = "Global_query";
    private static final String LOCAL_STAR_QUERY = "Local_query";
    private static final String JOIN_NODE = "join_node";
    private static final String UPDATE_NODE = "updated_node";
    private static final String DELETE_NODE = "deleted_node";
    private static final String INSERT_STAR = "insert_star";
    private static final String FOUND_STAR = "found_star";


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        int returnValue = 0;
        if(selection.matches("\"*\"")){
            returnValue = writeToFile();
            isDeleted = true;
            ClientTask client_task = new ClientTask();
            client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE_NODE, node.getSuccessor().getPortID());
            Log.v(TAG, "Deleting all data from current avd" + returnValue);
        } else if(selection.matches("\"@\"")){
            returnValue = writeToFile();
            Log.v(TAG, "Deleting all data from current avd" + returnValue);
        } else {
            writeToFile();
        }
        return returnValue;
    }

    private int writeToFile() {

        int returnValue = 0;
        try{
            File file = getContext().getFilesDir();
            File [] all_files = file.listFiles();

            for(File del_file : all_files) {
                getContext().deleteFile(del_file.getName());
                returnValue++;
            }

        } catch (Exception e) {
            Log.e("DELETE", "Deleting files from avd failed");
        }
        return returnValue;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String key_name = null;
        String key_hash = null;
        String value = null;

        try {
            isDeleted = false;
            for(String key: values.keySet()) {
                if(key.equals("key")){
                    key_name = (String)values.get(key);
                    key_hash = genHash(key_name);
                } else {
                    value = (String)values.get(key);
                }
            }
            Log.d(TAG, "The size of the chord is " + chord.size());

            if( chord.size() == 1 || (key_hash.compareTo(node.getPredecessor().getHashedValue()) > 0 && key_hash.compareTo(node.getHashedValue()) <= 0) || (node.getHashedValue().equals(genHashPort(node.getMinNode())) && ( key_hash.compareTo(node.getHashedValue()) <= 0 || key_hash.compareTo(genHashPort(node.getMaxNode())) > 0 ))) {
                Log.v("INSERT", key_name + " " + value);
                Log.v("TRUE_INSERT_" + key_hash, node.getPredecessor().getPortID() + " " + node.getPortID());

                FileOutputStream output = getContext().openFileOutput(key_name, Context.MODE_PRIVATE);
                output.write(value.getBytes());
                output.close();
            } else if( node.getHashedValue().equals(genHashPort(node.getMaxNode()) ) ) {
                new ClientTask().executeOnExecutor( AsyncTask.SERIAL_EXECUTOR, INSERT_STAR, node.getMinNode(), key_name + "%%" + value );
            } else {
                new ClientTask().executeOnExecutor( AsyncTask.SERIAL_EXECUTOR, INSERT_STAR, node.getSuccessor().getPortID(), key_name + "%%" + value );
            }

        } catch (Exception e) {
            Log.e(TAG, "Insert operation failed");
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        try {
            try {
                Log.d(TAG, "Creating a ServerSocket in onCreate");
                ServerSocket server_socket = new ServerSocket(SERVER_PORT);
                ServerTask server_task = new ServerTask();
                server_task.executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, server_socket);
            } catch (IOException e) {
                Log.e(TAG, "Failed to create a Server Socket in onCreate");
                e.printStackTrace();
                return false;
            }
            node = new Message();
            String myPort = getPort();
            getNode(myPort, myPort, myPort);
            Log.d(TAG, "Sending join request:");
            chord.add(AVD_JOIN_REQ);

            if(!myPort.equals(AVD_JOIN_REQ)) {

                try{
                    Log.d(TAG, "Creating ClientTask");
                    ClientTask client_task = new ClientTask();
                    client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, JOIN_NODE, AVD_JOIN_REQ, myPort);
                    Log.v("SENT_JOIN_REQUEST", "sent join request to " + AVD_JOIN_REQ);
                } catch (Exception e){
                    Log.e(TAG, "Failed to create a Client Task");
                    e.printStackTrace();
                }

            }


        }catch(Exception e){
            Log.e(TAG, "onCreate:Exception in SimpleDhtProvider");
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        String read_line = null;
        cursor = new MatrixCursor(cols);
        String value = null;
        FileInputStream input = null;
        String hashQuery = null;
        String result = null;
        String originator_port = selectionArgs==null? getPort():selectionArgs[0];

        try {

            if( selection.contains("*") ){
                Log.e(TAG, "Selection:"+ selection);

                if(selectionArgs != null){
                    result = selectionArgs[1];
                }
                else {
                    result = "";
                }
                for (String file : getContext().fileList()) {
                    input = getContext().openFileInput(file);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(input));

                    while ((read_line = bufferedReader.readLine()) != null) {
                        value = read_line;
                    }
                    result = result + file + "%" + value.toString() + "%";
                }
                if (node.getHashedValue().equals(genHashPort(node.getMaxNode())) && !node.getHashedValue().equals(genHashPort(node.getMinNode()))) {

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, GLOBAL_STAR_QUERY, node.getMinNode(), originator_port, result);
                    if(originator_port.equals(getPort())) {
                        while (!myQuery) {

                        }
                    }
                }

                else if (!node.getHashedValue().equals(node.getSuccessor().getHashedValue())) {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, GLOBAL_STAR_QUERY, node.getSuccessor().getPortID(), originator_port, result);
                    if(originator_port.equals(getPort())) {
                        while (!myQuery) {

                        }
                    }
                }

                else {
                    query_global = result;
                }

                myQuery = false;
                if(!query_global.isEmpty() && originator_port.equals(getPort())) {
                    query_global = query_global.trim().substring(0, query_global.length() - 1);
                    String[] globalValues = query_global.split("%");

                    StringTokenizer tokenizer = new StringTokenizer(query_global, "%");
                    while (tokenizer.hasMoreTokens()) {

                        String[] values = new String[]{tokenizer.nextToken(), tokenizer.nextToken()};
                        cursor.addRow(values);
                    }
                }


                return cursor;

            }

            else if( selection.matches("@")){
                for(String file : getContext().fileList()) {
                    input = getContext().openFileInput(file);
                    BufferedReader bufferedReader= new BufferedReader(new InputStreamReader(input));

                    while ((read_line = bufferedReader.readLine()) != null) {
                        value = read_line;
                    }
                    String[] values = new String[]{file, value.toString()};
                    cursor.addRow(values);
                    Log.d("Count", cursor.getCount() + "");
                }

                return cursor;
            }

            else {
                hashQuery = genHash(selection);

                if( chord.size() == 1 || (hashQuery.compareTo(node.getPredecessor().getHashedValue()) > 0 && hashQuery.compareTo(node.getHashedValue()) <= 0) || (node.getHashedValue().equals(genHashPort(node.getMinNode())) && ( hashQuery.compareTo(node.getHashedValue()) <= 0 || hashQuery.compareTo(genHashPort(node.getMaxNode())) > 0 ))) {
                    Log.d(TAG,"Predecessor Node is "+ node.getPredecessor().getPortID());

                    input = getContext().openFileInput(selection);
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(input));

                    while ((read_line = bufferedReader.readLine()) != null) {
                        value = read_line;
                    }


                    if( originator_port.equals(getPort()) ) {
                        Log.e(TAG, "Originator_port"+ originator_port);
                        String[] values = new String[]{selection, value.toString()};
                        cursor.addRow(values);
                        Log.d("Count", cursor.getCount() + "");
                        return cursor;
                    }

                    else {

                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, LOCAL_STAR_QUERY, originator_port, originator_port, selection, FOUND_STAR, value.toString());
                    }
                }

                else if( node.getHashedValue().equals( genHashPort(node.getMaxNode()) )  && !node.getHashedValue().equals(genHashPort(node.getMinNode()))) {

                    new ClientTask().executeOnExecutor( AsyncTask.SERIAL_EXECUTOR, LOCAL_STAR_QUERY, node.getMinNode(), originator_port, selection,"NotFound"," " );
                    if(originator_port.equals(getPort())) {
                        Log.d(TAG, "Current port" + getPort());

                        while (!myQuery) {
                            //do nothing
                        }
                    }
                }  else if (!node.getHashedValue().equals(node.getSuccessor().getHashedValue())) {
                    Log.d(TAG,"Successor Node is "+ node.getSuccessor().getPortID());

                    new ClientTask().executeOnExecutor( AsyncTask.SERIAL_EXECUTOR, LOCAL_STAR_QUERY, node.getSuccessor().getPortID(), originator_port, selection,"NotFound"," " );
                    if(originator_port.equals(getPort())) {
                        Log.d(TAG, "Current port" + getPort());

                        while (!myQuery) {
                            //do nothing
                        }
                    }
                }


                myQuery = false;
                if(checkData == null) {
                    return cursor;
                } else {
                    String[] values = new String[]{selection.trim(), checkData.trim()};
                    cursor.addRow(values);
                    Log.d("Count", cursor.getCount() + "");
                    return cursor;
                }
            }
        } catch (Exception e) {
            Log.e(TAG, e.getMessage());
            Log.e(TAG, selection);
            e.printStackTrace();
        }
        return cursor;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        private Uri myUri;

        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            String msgDelimiter[];
            StringBuilder returnValue = new StringBuilder();
            String resultString = null;
            String nodeParams[];

            myUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
            try {
                while(true) {
                    Log.v(TAG, "Creating server connection..");
                    Socket readSocket = serverSocket.accept();
                    BufferedReader br = new BufferedReader(new InputStreamReader(readSocket.getInputStream()));
                    String msgToRecv;
                    while((msgToRecv = br.readLine())!=null) {
                        Log.v(TAG, "Message received type: Join node");
                        if(msgToRecv.contains(JOIN_NODE)){
                            msgDelimiter = msgToRecv.split( " " );
                            isBetween(msgDelimiter[1]);


                            if(chord.size() != 0) {
                                Log.d(TAG, "The size of the chord is " + chord.size());
                                PrintWriter printWriter = new PrintWriter(readSocket.getOutputStream(), true);
                                Log.d(TAG, "Connected successfully.");
                                for (String formChord : chord) {
                                    returnValue.append(formChord);
                                    returnValue.append(" ");
                                    resultString = returnValue.toString();
                                }
                                printWriter.println(resultString);
                            }
                        }

                        else if(msgToRecv.contains(INSERT_STAR)){
                            Log.d(TAG, "Message received type: insert");

                            msgToRecv = msgToRecv.replace(INSERT_STAR, "");
                            //String delim = "%%";

                            ContentValues val = new ContentValues();
                            StringTokenizer stringTokenizer = new StringTokenizer(msgToRecv,"%%");

                            Log.d(TAG, "Inserting key-value pairs");
                            while(stringTokenizer.hasMoreTokens()) {

                                val.put("key", stringTokenizer.nextToken());
                                val.put("value", stringTokenizer.nextToken());

                                getContext().getContentResolver().insert(myUri, val);
                            }
                        }

                        else if(msgToRecv.contains(DELETE_NODE)){
                            Log.d(TAG, "Message received type: Delete node");
                            getContext().getContentResolver().delete(myUri, "\"*\"", null);

                        }

                        else if(msgToRecv.contains(UPDATE_NODE)){
                            Log.d(TAG, "Message received type: Update node");
                            msgDelimiter = msgToRecv.split( " " );
                            isBetween(msgDelimiter[1]);

                        }


                        else if(msgToRecv.contains(GLOBAL_STAR_QUERY)){
                            Log.v(TAG, "Message received type: Query star global");
                            msgDelimiter = msgToRecv.split("@@");

                            if(msgDelimiter[1].equals(getPort())){

                                if(msgDelimiter.length > 2) {
                                    query_global = msgDelimiter[2];
                                }
                                myQuery = true;
                            }
                            else {
                                if(msgDelimiter.length > 2) {
                                    nodeParams = new String[]{msgDelimiter[1], msgDelimiter[2]};
                                }else{
                                    nodeParams = new String[]{msgDelimiter[1], ""};
                                }
                                getContext().getContentResolver().query(myUri, null, "*", nodeParams, null);
                            }
                        }

                        else if(msgToRecv.contains(LOCAL_STAR_QUERY)){
                            Log.d(TAG, "Message received type: query local");


                            msgDelimiter = msgToRecv.split("@@");
                            if( msgDelimiter[3].equals(FOUND_STAR) ){
                                Log.d(TAG, "Message received type: Found *");

                                checkData = msgDelimiter[4];
                                myQuery = true;
                            } else if(msgDelimiter[1].equals(getPort()) ){
                                myQuery = true;
                            } else {
                                nodeParams = new String[]{msgDelimiter[1]};
                                getContext().getContentResolver().query(myUri, null, msgDelimiter[2], nodeParams, null);
                            }
                        }
                    }
                }
            }catch(IOException e){
                Log.e(TAG, Log.getStackTraceString(e));

            }catch(Exception e){
                Log.e(TAG, Log.getStackTraceString(e));
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {

            String strReceived = strings[0].trim();
            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            Socket socket = null;
            Socket socket1 = null;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1]));
                String msgToSend = msgs[0];

                if(msgToSend.contains(JOIN_NODE)){
                    Log.d(TAG, "Sending join request to first port: 5554");
                    msgToSend = msgToSend + " " + msgs[2];
                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println(msgToSend);

                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String formChord[];
                    String msgToRecv = null;
                    msgToRecv = reader.readLine();
                    if (msgToRecv != null) {

                        formChord = msgToRecv.split(" ");
                        for( String node : formChord ) {
                            Log.d(TAG, "Size of node:" + formChord.length);
                            if(node.equals(AVD_JOIN_REQ) || node.equals(msgs[2])){
                                continue;
                            }
                            Log.v(TAG, "Creating socket..");
                            try{
                                socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node));
                                printWriter = new PrintWriter(socket1.getOutputStream(), true);
                                msgToSend = UPDATE_NODE + " " + msgs[2];
                                printWriter.println(msgToSend);
                            }catch (UnknownHostException e){
                                e.printStackTrace();
                            }

                        }
                        for( String node : formChord ){
                            isBetween(node);
                        }
                    }
                }
                else if(msgToSend.contains(DELETE_NODE)) {
                    Log.d(TAG, "Sending msg to delete");
                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println(msgToSend);
                }

                else if(msgToSend.contains(INSERT_STAR)) {
                    Log.d(TAG, "Sending msg to insert");
                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println( INSERT_STAR + msgs[2]);
                }
                else if(msgToSend.contains(LOCAL_STAR_QUERY)) {
                    Log.d(TAG, "Sending msg to query star local");
                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println( msgToSend + "@@" + msgs[2] + "@@" + msgs[3] + "@@" +msgs[4] + "@@" + msgs[5] );

                }
                else if(msgToSend.contains(GLOBAL_STAR_QUERY)) {
                    Log.d(TAG, "Sending msg to query star global");
                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true );
                    printWriter.println( msgToSend + "@@" + msgs[2] + "@@" + msgs[3] );

                }


               if(socket1 != null) {
                    Log.v(TAG, "Close connection1");
                    socket1.close();
                }
                if(socket != null) {
                    Log.v(TAG, "Close connection");
                    socket.close();
                }


            } catch (IOException e) {
                Log.e(TAG, "IOException");
                e.printStackTrace();
            } catch (Exception e){
                Log.e(TAG, "Exception");
                e.printStackTrace();
            }

            return null;
        }
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

    private String genHashPort(String input) throws NoSuchAlgorithmException {
        input = String.valueOf(Integer.parseInt(input)/2);
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private void getNode(String nodeID, String predecessor, String successor){


        try {

            node.setSuccessor(successor_node);
            node.setMin_node(nodeID);
            node.setMax_node(nodeID);

            node.setPortID(nodeID);
            node.setHashVal(genHashPort(nodeID));

            predecessor_node.setPortID(nodeID);
            predecessor_node.setHashVal(genHashPort(nodeID));

            node.setPredecessor(predecessor_node);

            successor_node.setPortID(nodeID);
            successor_node.setHashVal(genHashPort(nodeID));

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    protected void onProgressUpdate(String...msgs) {
            /*
             * The following code displays what is received in doInBackground().
             */
        //From PA1
        query(null,null, msgs[0], new String[]{msgs[1]}, null);
    }

    private void isBetween(String nodeId){

        try {
            String key_hash = genHashPort( nodeId );
            Log.v(TAG,"New Node is "+ nodeId);

            if( key_hash.compareTo(node.getHashedValue()) > 0){
                Log.d(TAG, "This Node is " + node.getPortID());
                if((node.getHashedValue().equals(node.getSuccessor().getHashedValue()) && key_hash.compareTo(node.getSuccessor().getHashedValue()) > 0) ||
                        key_hash.compareTo(node.getSuccessor().getHashedValue()) < 0) {
                    Log.d(TAG,"Successor Node is "+ node.getSuccessor().getPortID());
                    node.setSuccessor(new Message());
                    node.getSuccessor().setPortID(nodeId);
                    node.getSuccessor().setHashVal(key_hash);
                }
            }
            else if( key_hash.compareTo(node.getHashedValue()) < 0 ){
                if((node.getHashedValue().equals(node.getPredecessor().getHashedValue()) && key_hash.compareTo(node.getPredecessor().getHashedValue()) < 0) ||
                        key_hash.compareTo(node.getPredecessor().getHashedValue()) > 0) {
                    Log.d(TAG,"Predecessor Node is "+ node.getPredecessor().getPortID());
                    node.setPredecessor(new Message());
                    node.getPredecessor().setPortID(nodeId);
                    node.getPredecessor().setHashVal(key_hash);
                }
            }

            if(key_hash.compareTo( genHashPort( node.getMaxNode() ) ) > 0){
                Log.d(TAG,"Max node is : "+ node.getMaxNode());
                node.setMax_node(nodeId);
            }

            else if(key_hash.compareTo( genHashPort( node.getMinNode() ) ) < 0){
                Log.d(TAG,"Min node is : "+ node.getMinNode());
                node.setMin_node(nodeId);
            }

            chord.add(nodeId);

        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG,Log.getStackTraceString(e));
        } catch (Exception e){
            Log.e(TAG, "Failed to get information");
            e.printStackTrace();
        }
    }

    private String getPort() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        return String.valueOf((Integer.parseInt(portStr) * 2));
    }

}
