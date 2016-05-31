/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package maptable;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author dcahalane
 */
public class FileMap implements Serializable{

    final static byte START = (byte) 1;
    final static byte DELIM = (byte) 2;
    final static byte END = (byte) 3;
    File file;
    RandomAccessFile raf;
    FileOutputStream out;
    SortedMap<String, long[]> keyIndex = new TreeMap<String, long[]>();
    
    
    /*
        
    */
    static FileMap _open(FileMap newMap) throws FileNotFoundException, IOException{
        String directory = newMap.file.getParent();
        String name = newMap.file.getName();//Remove the .tmp;
        name = name.substring(0, name.length()-4);

        FileInputStream in = new FileInputStream(new File(directory, name));
        FileChannel ch = in.getChannel( );
        byte[] ioBuf = new byte[0];
        ByteBuffer bb = ByteBuffer.allocateDirect(64*1024);
        bb.clear();
        //ByteBuffer bb = ByteBuffer.wrap( ioBuf );
        int bytesRead = 0;
        
        boolean keyFlag = false, valueFlag = false;
        byte[] keyBuffer = null, valueBuffer = null;
        byte b = 0;
        //while ((bytesRead = in.read(ioBuf)) != -1) {
        while ( (bytesRead=ch.read( bb )) != -1 ){
            bb.flip();
            //for (int i = 0; i < bytesRead; i++) {
            while (bb.hasRemaining()){
                b = bb.get();
                if (b == START) {//ioBuf[i]
                    keyFlag = true;
                } else if (b == DELIM) {
                    valueFlag = true;
                    keyFlag = false;
                } else if (b == END) {
                    keyFlag = false;
                    valueFlag = false;
                    
                    newMap.put(new String(keyBuffer), new String(valueBuffer));
                    keyBuffer = null;
                    valueBuffer = null;
                } else {
                    if (keyFlag == true) {
                        if (keyBuffer == null) {
                            keyBuffer = new byte[]{b};
                        } else {
                            keyBuffer = mergeBytes(keyBuffer, b);
                        }
                    } else if (valueFlag == true) {
                        if (valueBuffer == null) {
                            valueBuffer = new byte[]{b};
                        } else {
                            valueBuffer = mergeBytes(valueBuffer, b);
                        }
                    } else {
                        //It ended, but a new one didn't start, so we may as well die here???
                    }
                }

            }
            //ioBuf = new byte[32];
            bb.clear( );
        }
        in.close();
        newMap.moveFile(new File(directory, name));
        return newMap;
    }

    /*
    The problem is that multiple FileMaps can be opened which all point to the same underlying file.  
    Need a MapBroker to handle calls to open or create a map.  
    */
    public static FileMap open(String directory, String name) throws FileNotFoundException, IOException {
        //Read in the data within the file and rebuild the keyIndex map.
        FileMap retMap = new FileMap(directory, name + ".tmp");
        return _open(retMap);
    }


    public FileMap(File file) throws IOException {
        this.file = file;
        raf = new RandomAccessFile(file, "rw");
        this.out = new FileOutputStream(file, false);
    }

    public FileMap(String directory, String name) throws IOException {
        this(new File(directory, name));
    }

    public FileMap(String directory) throws IOException {
        file = new File(directory, this.hashCode() + ".map");
        raf = new RandomAccessFile(file, "rw");
        out = new FileOutputStream(file, false);
    }

    private void moveFile(File newFile) throws FileNotFoundException, IOException {

        newFile.delete();//Make sure the new file doesn't exist
        Files.move(this.file.toPath(), newFile.toPath(), StandardCopyOption.REPLACE_EXISTING);//Move the old one to the new one.
        this.file = newFile;
        this.out.close();
        this.out = new FileOutputStream(this.file, true);
        this.raf.close();
        this.raf = new RandomAccessFile(file, "rw");
    }

    public void close() throws IOException {
        out.close();
        raf.close();
    }

    /*
     Updates of the map will leave older entries stranded.  (The keyIndex map will no longer point to them.
     Coalescing the file will eliminate these stranded entries and condense the file.
     */
    public FileMap coalesce() throws IOException, NoSuchMethodException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        //Read in the current data within keyIndex, and write it out to a new output file.
        File tempFile = new File(this.file.getParentFile().getPath(), this.file.getName() + ".tmp");
        Class[] argC = new Class[2];
        argC[0] = File.class;
        argC[1] = Set.class;
                
        FileMap newMap =  this.getClass().getDeclaredConstructor(argC).newInstance(tempFile, ((JsonMap)this).fieldIndex.keySet());
        for (String key : this.keyIndex.keySet()) {
            newMap.put(key.toString(), this.get(key.toString()));
        }
        newMap.moveFile(new File(this.file.getParentFile().getPath(), this.file.getName()));
        tempFile.delete();
        return newMap;
    }

    /*
     This just continually adds the key:value pair to the end of the file.
     The keyIndex map is updated to account for the updated entry.
     */
    public void put(String key, String value) throws IOException {
        //Convert KEY : VALUE to bytes
        synchronized (this.out) {
            long[] indexInfo = _put(key, value);
            //Update keyIndex with key + location
            if(indexInfo != null){
                this.keyIndex.put(key, indexInfo);
            }
        }
    }

    long[] _put(String key, String value) throws IOException {
        
        byte[] data = mergeBytes(new byte[]{START}, key.getBytes());
        data = mergeBytes(data, DELIM);
        data = mergeBytes(data, value.getBytes());
        data = mergeBytes(data, END);
        //Find last open location in out
            long location = file.length();
            write(data);
            return new long[]{location, data.length};
    }

    void write(byte[] data) throws IOException {
        //TODO: Write this out to an alternate location as well.
        synchronized(file){
            this.out.write(data);
            this.out.flush();
        }
    }

    public String get(String key) throws IOException {
        //Get the location to read.
        synchronized (this.out) { // Locks on the output stream.
            return _get(this.keyIndex, key);
        }

    }

    public String _get(SortedMap<String, long[]> index, String key) throws IOException {
        if(key != null){
        long[] location = index.get(key);
        if (location == null) {
            return null;
        }
        raf.seek(location[0] + 1 + key.length() + 1);//Dont read the pair separator
        byte[] buffer = new byte[(int) location[1] - 2 - key.length() - 1];
        raf.read(buffer);
        return new String(buffer);
        }else return null;
    }

    public Set<String> keySet() {
        return keyIndex.keySet();
    }

    static byte[] mergeBytes(byte[] one, byte[] two) {
        byte[] retVal = new byte[one.length + two.length];
        System.arraycopy(one, 0, retVal, 0, one.length);
        System.arraycopy(two, 0, retVal, one.length, two.length);
        return retVal;
    }
    static byte[] mergeBytes(byte[] one, byte two){
        byte[] retVal = new byte[one.length + 1];
        System.arraycopy(one, 0, retVal, 0, one.length);
        retVal[retVal.length-1] = two;
        return retVal;
    }

    public static void main(String[] args) {
        try {
            long start = System.currentTimeMillis();
            FileMap map = FileMap.open("/home/dcahalane", "17689166.map");

            System.out.println("File Length " + map.file.length());
            map.put("TEST-KEY", "{TEST VALUE}");
            System.out.println("File Length " + map.file.length());

        } catch (IOException ex) {
            Logger.getLogger(FileMap.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
    
    /*
    class CharArray implements Comparable{
        char[] array;
        public CharArray(String value){
            this.array = value.toCharArray();
        }
        public boolean equals(Object o){
            if(!(o instanceof char[])){
                return false;
            }
            return Arrays.equals(this.array, (char[])o);
        }
        public String toString(){
            return new String(array);
        }
        public int hashCode(){
            return toString().hashCode();
        }

        @Override
        public int compareTo(Object o) {
            if(o == null) return -1;
            return this.toString().compareTo(o.toString());
        }
    }
    */

}
