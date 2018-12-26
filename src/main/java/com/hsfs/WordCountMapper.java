package com.hsfs;

import com.hsfs.common.myContext;
import com.hsfs.dao.Mapper;

public class WordCountMapper implements Mapper {

    @Override
    public void map(String line, myContext myContext) {
        String[] words = line.split(" ");


        for (String word : words) {

            Object value = myContext.get(word);
            if(null==value){
                myContext.write(word,1);
            }else {
               int  num=(int)value;
                myContext.write(word,num+1);
            }


        }



    }

}
