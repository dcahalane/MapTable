/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package maptable.db;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author dcahalane
 */
public class MapBroker {

    String serverName;
    int port;
    String mapName;

    public MapBroker(String serverName, int port, String mapName) {
        this.serverName = serverName;
        this.port = port;
        this.mapName = mapName;
    }

    public Serializable get(Serializable key) throws IOException, ClassNotFoundException {
        MapCommand command = new MapCommand(this.mapName, MapCommand.GET, new Serializable[]{key});
        //Connect to the MapListener.
        //Pass Up the MapCommand
        //Get back the Serializable element contained within the FileMap/JsonMap.
        Serializable retObj = _connect(command);
        if(retObj instanceof IOException){
            throw (IOException)retObj;
        }
        return retObj;
    }

    public void put(Serializable key, Serializable object) throws IOException, ClassNotFoundException {
        MapCommand command = new MapCommand(this.mapName, MapCommand.PUT, new Serializable[]{key,object});
        Serializable retObj = _connect(command);
        if(retObj instanceof IOException){
            throw (IOException)retObj;
        }
    }

    public List<Serializable> find(Map<String, String[]> lookup) throws IOException, ClassNotFoundException {
        MapCommand command = new MapCommand(this.mapName, MapCommand.FIND, lookup);
        //Connect to the MapListener.
        //Pass Up the MapCommand
        //Get back the Serializable element contained within the FileMap/JsonMap.
        Serializable retObj = _connect(command);
        if(retObj instanceof String[]){
            return Arrays.asList((String[])retObj);
        }else if(retObj instanceof IOException){
            throw (IOException)retObj;
        }else{
            System.out.println("## " + retObj.getClass().getName());
        }
        return null;
    }

    Serializable _connect(MapCommand command) throws IOException, ClassNotFoundException {
        //Connect to the server
        Socket clientSocket = new Socket(this.serverName, this.port);
        OutputStream os = clientSocket.getOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(os);
        oos.writeObject(command);

        ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
        Object inbound = in.readObject();
        
        oos.close();
        os.close();
        clientSocket.close();
        return (Serializable) inbound;
    }

    public static void main(String[] args) {
        try {
            Runnable server = new Runnable() {
                public void run() {
                    try {
                        MapListener.start(new String[]{"1234"});
                    } catch (IOException ex) {
                        Logger.getLogger(MapBroker.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            };
            Thread t = new Thread(server);
            t.start();
            Thread.sleep(5000);

            MapBroker broker = new MapBroker("localhost", 1234, "document.map");  //Creates a new MapBroker.


            
            
            class Tester implements Runnable {


                int base = 1012;
                int i;
                MapBroker broker;

                public Tester(int i, MapBroker broker) {
                    this.i = i;
                    this.broker = broker;
                }

                public void run() {

                    Serializable o;
                    try {

                        o = this.broker.get("" + (base + i));
                        if(o != null){
                            System.out.println("###" + (base + i) + "\n" + o.toString() + "\n###");
                            this.broker.put("" + (base + i), o);
                            STORED = o;
                        }else{
                            System.out.println("No Data Found " + (base + i));
                            Serializable s = STORED;
                            while(s == null){
                                try {
                                    Thread.sleep(500);
                                } catch (InterruptedException ex) {
                                    Logger.getLogger(MapBroker.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                s = STORED;
                            }
                            if(s != null){
                                System.out.println("Putting new record for " + (base + i));
                                this.broker.put("" + (base + i), s);
                            }
                        }
                    } catch (IOException ex) {
                        Logger.getLogger(MapBroker.class.getName()).log(Level.SEVERE, null, ex);
                    } catch (ClassNotFoundException ex) {
                        Logger.getLogger(MapBroker.class.getName()).log(Level.SEVERE, null, ex);
                    }

                }
            };
            /*
            for (int j = 1; j < 12; j++) {
                for (int i = 0; i < 6; i++) {
                    Thread tt = new Thread(new Tester(i*j, broker));
                    tt.start();
                }
            }
            */
            Map<String, String[]> lookup = new TreeMap<String, String[]>();
            lookup.put("docId", new String[]{"1012", "1013"});
            List<Serializable> retVal = broker.find(lookup);
                for(Object d: retVal){
                    System.out.println((String)d);
                }

        } catch (Exception ex) {
            Logger.getLogger(MapBroker.class.getName()).log(Level.SEVERE, null, ex);
        }

        //broker.get(key);//Connects to the MapListener, passes in the MapCommand and returns the serialized object stored in the map.
    }
    
    static Serializable STORED = null;
}
