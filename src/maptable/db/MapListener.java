/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package maptable.db;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import maptable.FileMap;

/**
 *
 * @author dcahalane
 */
public class MapListener implements Runnable {

    static int MAX_CONNECTIONS = 1000;
    static int LISTEN_PORT = 1234;

    public static void main(String[] args) {
        try {
            MapListener.start(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(9);
        }
    }

    public static void start(String[] args) throws IOException {
        if (args.length > 1) {
            MAX_CONNECTIONS = Integer.parseInt(args[1]);
        }
        if (args.length > 0) {
            LISTEN_PORT = Integer.parseInt(args[0]);
        }

        int currentConnections = 0;

        try {
            ServerSocket listener = null;
            try {
                listener = new ServerSocket(LISTEN_PORT);
            } catch (Exception e) {
                throw (new IOException("Listener already running"));
            }
            Socket server;
            System.out.println("MapListener running on port " + LISTEN_PORT);
            while ((currentConnections++ < MAX_CONNECTIONS) || (MAX_CONNECTIONS < 0)) {
                MapListener connection;
                try {
                    server = listener.accept();
                    MapListener conn_c = new MapListener(server);
                    Thread t = new Thread(conn_c);
                    t.start();
                } catch (Exception eComms) {
                    eComms.printStackTrace();
                }
            }
            throw new IOException("Current Connections exceed MAX_CONNECTIONS of " + MAX_CONNECTIONS);
        } catch (IOException e) {
            System.out.println("IOException on socket listen: " + e);
            throw e;
        }
    }

    private Socket server;
    private String line, input;

    MapListener(Socket server) {
        this.server = server;
    }

    String[] convertToString(Serializable[] input) {
        if(input == null) return null;
        String[] retVal = new String[input.length];
        for (int i = 0; i < retVal.length; i++) {
            retVal[i] = (String) input[i];
        }
        return retVal;
    }

    Class[] convertToClass(Serializable[] input) {
        Class[] retVal = new Class[input.length];
        for (int i = 0; i < retVal.length; i++) {
            retVal[i] = input[i].getClass();
        }
        return retVal;
    }

    public void run() {
        input = "";
        try {
            // Get input from the client
            ObjectInputStream in = new ObjectInputStream(server.getInputStream());
            Object inbound = in.readObject();
            FileMap map = null;
            if (inbound instanceof MapCommand) {
                map = MapManager.getInstance(((MapCommand) inbound).mapName);
                //System.out.println(inbound.toString());

                //inbound is the name of the FileMap/JsonMap. 
                //TODO: This should be done with reflection.
                if (map != null) {
                    String mapMethodName = (((MapCommand) inbound).command);
                    String[] params = convertToString(((MapCommand) inbound).parameters);
                    Map<String, String[]> lookup = ((MapCommand) inbound).convertToMap(((MapCommand) inbound).lookup);
                    Serializable result = null;
                    if (params != null) {
                        Method mapMethod = map.getClass().getMethod(mapMethodName, convertToClass(((MapCommand) inbound).parameters));
                        try {
                            result = (String) mapMethod.invoke(map, ((MapCommand) inbound).parameters);
                        } catch (Exception e) {
                            result = e;
                        }
                    }else if(lookup != null){
                        Method mapMethod = map.getClass().getMethod(mapMethodName, Map.class);
                        try {
                            Object r = mapMethod.invoke(map, lookup);
                            ArrayList<String> lookupResult = (ArrayList<String>)r;
                            result = (String[])lookupResult.toArray(new String[]{});
                        } catch (Exception e) {
                            result = e;
                        }
                    }
                    OutputStream os = server.getOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(os);
                    oos.writeObject(result);
                    oos.close();
                    os.close();
                }
                in.close();
            }

            server.close();
        } catch (Exception ioe) {
            System.out.println("IOException on socket listen: " + ioe);
            ioe.printStackTrace();

        }
    }

}
