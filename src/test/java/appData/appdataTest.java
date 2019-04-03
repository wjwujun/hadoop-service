package appData;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class appdataTest {
    public class AA{
        String name;
        String sex;
        int  age;

        public AA(String name, String sex, int age) {
            this.name = name;
            this.sex = sex;
            this.age = age;
        }
    }
    @Test
    public void stringJson(){
        AA aa = new AA("aa", "男", 11);
        /*数据源为json*/
        //JSONObject jsonObj = JSON.parseObject(aa.toString());

        //System.out.println(jsonObj);
        System.out.println("1111111111111");
    }
}


