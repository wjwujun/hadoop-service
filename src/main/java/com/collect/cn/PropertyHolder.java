package com.collect.cn;

import java.io.IOException;
import java.util.Properties;

/*懒汉单列模式*/
public class PropertyHolder {

    private static Properties prop=null;

    public  static  Properties getProps() throws IOException {
        if (prop==null){
            synchronized(PropertyHolder.class){  //加锁
                if (prop==null){
                    prop = new Properties();
                    //类加载器
                    prop.load(PropertyHolder.class.getClassLoader().getResourceAsStream("collect.properties"));
                }
            }
        }
        return  prop;
    }
}
