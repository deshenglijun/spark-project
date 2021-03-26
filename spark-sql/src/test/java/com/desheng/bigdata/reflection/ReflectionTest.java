package com.desheng.bigdata.reflection;

import com.desheng.bigdata.spark.domain.Person;

import java.lang.reflect.Field;
public class ReflectionTest {
    public static void main(String[] args) {
        Person person = new Person("韩香彧", 17, 167.5);
        Class<? extends Person> clazz = person.getClass();

        Field[] fields = clazz.getDeclaredFields();

        for(Field field : fields) {
            String name = field.getName();
            Class<?> type = field.getType();
//            private int age;
            System.out.println(type + " " + name );
        }

    }
}
