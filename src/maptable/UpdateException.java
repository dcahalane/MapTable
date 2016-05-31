/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package maptable;

import java.io.Serializable;

/**
 *
 * @author dcahalane
 */
public class UpdateException extends Exception implements Serializable{
    public UpdateException(){
        super();
    }
    public UpdateException(Exception e){
        super(e.getMessage());
    }
}
