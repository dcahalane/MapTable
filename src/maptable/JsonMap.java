/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package maptable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author dcahalane
 */
public class JsonMap extends FileMap {

    TreeMap<String, SortedMap<String, List<String>>> fieldIndex = new TreeMap<String, SortedMap<String, List<String>>>();
    TreeMap<FieldDescriptor, String> valueMap = new TreeMap<FieldDescriptor, String>();

    JSONParser parser = new JSONParser();

    public JsonMap(File file, Set<String> indexNames) throws IOException {
        super(file);
        this.setIndexes(indexNames);
    }

    public JsonMap(String directory, String name, Set<String> indexNames) throws IOException {
        super(directory, name);
        this.setIndexes(indexNames);
    }

    public JsonMap(String directory, Set<String> indexNames) throws IOException {
        super(directory);
        this.setIndexes(indexNames);
    }

    void setIndexes(Set<String> indexNames) {
        for (String i : indexNames) {
            this.fieldIndex.put(i, new TreeMap<String, List<String>>());
        }
    }

    public static JsonMap open(String directory, String name, Set<String> indexNames) throws FileNotFoundException, IOException {
        JsonMap retMap = new JsonMap(directory, name + ".tmp", indexNames);
        return (JsonMap) _open(retMap);
    }

    /*
     Indexes the primary field to the document's location then fieldIndex the fields 
     against the doc-id.
     //TODO:  What if the fields value has changed???
     */
    public synchronized void put(String key, String value) throws IOException {
        //synchronized (this.out) {
//System.out.println("PUTTING DATA FOR " + key);
            try {
                JSONObject doc = (JSONObject) parser.parse(value);
                long[] indexInfo = _put(key, value);
                this.keyIndex.put(key, indexInfo);
                for (String fieldName : this.fieldIndex.keySet()) {
                    String fieldValue = (String) doc.get(fieldName);
                    if (fieldValue != null && fieldValue.length() > 0) {
                        FieldDescriptor fD = new FieldDescriptor(key, fieldName);
                        //Clears the old index
                        String oldValue = valueMap.get(fD);
                        if (oldValue != null && oldValue.length() > 0) {
                            List<String> old = fieldIndex.get(fieldName).get(oldValue);
                            if (old != null) {
                                old.remove(key);
                            }
                        }
                        //Adds in the new index
                        List<String> docIds = fieldIndex.get(fieldName).get(fieldValue);
                        if (docIds == null) {
                            docIds = new ArrayList<String>();
                            fieldIndex.get(fieldName).put(fieldValue, docIds);
                        }
                        docIds.add(key);
                        //Sets the current value
                        valueMap.put(fD, fieldValue);
                    }
                }
            } catch (ParseException ex) {
                Logger.getLogger(JsonMap.class.getName()).log(Level.SEVERE, null, ex);
                

            }
        //}
    }

    
    Set<String> _find(Map<String, String[]> lookup) throws IOException{
        Set<String> mergedDocIds = null;
        //synchronized (this.out) {
            
            for (String indexName : lookup.keySet()) {
                SortedMap<String, List<String>> indexMap = this.fieldIndex.get(indexName);
                String[] values = lookup.get(indexName);
                Set<String> docIds = new TreeSet<String>();
                if (values != null) {
                    for (int i = 0; i < values.length; i++) {
                        List<String> r = indexMap.get(values[i]);
                        if (r != null) {
                            docIds.addAll(r);
                        }
                    }
                }
                if (mergedDocIds == null) {//first time through
                    mergedDocIds = new TreeSet<String>();
                    mergedDocIds.addAll(docIds);
                } else {
                    mergedDocIds.retainAll(docIds);
                }
            }
        //}
        return mergedDocIds;
    }
    /*
     The map contains the field-name and the values of that field to match.
     */
    public List<String> find(Map<String, String[]> lookup) throws IOException {
System.out.println("Calling FIND");
        List<String> retVal = new ArrayList<String>();
            Set<String> mergedDocIds = _find(lookup);
            for (String docId : mergedDocIds) {
                retVal.add(get(docId));
            }
            System.out.println("RESULTS of " + retVal.size());
        return retVal;
    }

    class FieldDescriptor implements Comparable {

        String id;
        String fieldName;

        public FieldDescriptor(String key, String name) {
            this.id = id;
            this.fieldName = name;
        }

        public boolean equals(Object o) {
            return this.toString().equals(o.toString());
        }

        public String toString() {
            return id + ":" + fieldName;
        }

        public int hashCode() {
            return this.toString().hashCode();
        }

        @Override
        public int compareTo(Object o) {
            if (o == null) {
                return -1;
            }
            return this.toString().compareTo(o.toString());
        }

    }

}
