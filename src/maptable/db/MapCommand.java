/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package maptable.db;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author dcahalane
 */
public class MapCommand implements Serializable{
    static final String GET = "get";
    static final String PUT = "put";
    static final String FIND = "find";
    
    String mapName;
    String command;
    Serializable[] parameters;
    Mapper[] lookup;
    
    /*
    This is for creating a get or a put.
    */
    public MapCommand(String mapName, String command, Serializable[] parameters){
        this.mapName = mapName;
        this.command = command;
        this.parameters = parameters;
    }
    public MapCommand(String mapName, String command, Map<String, String[]> lookup){
        this.mapName = mapName;
        this.command = command;
        this.lookup = convertFromMap(lookup);
    }
    
    public String toString(){
        String params = "";
        for(int i=0; i<parameters.length; i++){
            params = params + parameters[i].toString();
        }
        return mapName + "\t" + command + "\t" + params;
    }
    
    Mapper[] convertFromMap(Map<String, String[]> map){
        Mapper[] retVal = new Mapper[map.keySet().size()];
        int i = 0;
        for(String key: map.keySet()){
            Mapper m = new Mapper(key, map.get(key));
            retVal[i++] = m;
        }
        return retVal;
    }
    Map<String, String[]> convertToMap(Mapper[] mapper){
        TreeMap<String, String[]> retVal = new TreeMap<String, String[]>();
        for(Mapper m: mapper){
            retVal.put(m.key, m.fields);
        }
        return retVal;
    }
    
    class Mapper implements Serializable{
        String key;
        String[] fields;
        
        public Mapper(String k, String[] fields){
            this.key = k;
            this.fields = fields;
            
        }
    }

}
