/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package maptable.db;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import maptable.FileMap;
import maptable.JsonMap;

/**
 *
 * @author dcahalane
 */
public class MapManager {
    
    static Map<String, FileMap> INSTANCE = new TreeMap<String, FileMap>();
    
    public static synchronized FileMap getInstance(String name) throws IOException{
        FileMap retVal = INSTANCE.get(name);
        String dir = "/home/dcahalane";
        if(retVal == null){
                File mapFile = new File(dir, name);
                Set<String> indexFields = new HashSet<String>();
                indexFields.add("POR.Vendor");
                indexFields.add("POR.Site");
                indexFields.add("docId");
                if (mapFile.exists()) {
                    //Open the existing map
                    //TODO: We are assuming its a JSONMAP.  This needs to come from a config file.
                    retVal = JsonMap.open(dir, name, indexFields);
                } else {
                    //Create a new Map.
                    System.out.println("##### Creating new datastore");
                    //TODO: Don't assume its a JsonMap.  Get the type from a config file.
                    retVal = new JsonMap(mapFile, indexFields);
                }
            INSTANCE.put(name, retVal);
        }
        return retVal;
    }
}
