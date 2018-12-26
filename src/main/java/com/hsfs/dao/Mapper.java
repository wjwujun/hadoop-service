package com.hsfs.dao;


import com.hsfs.common.myContext;

public interface Mapper {

    //将这一行的结果放入一个缓存
    public void map(String line, myContext myContext);


}
