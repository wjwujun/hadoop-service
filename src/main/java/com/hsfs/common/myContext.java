package com.hsfs.common;

import java.util.HashMap;

public class myContext {

    private HashMap<Object,Object>  contextMap=new HashMap();

    public void write(Object key,Object value){

        contextMap.put(key,value);
    }

    public Object get(String key){
        return  contextMap.get(key);
    }

    public HashMap<Object,Object> getContextMap(){
        return contextMap;
    }

}
