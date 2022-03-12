package com.cintel.time;

import org.junit.Test;

import java.util.Random;

public class SystemCurrent {

    @Test
    public void test1() throws InterruptedException {
        for (int i = 0; i < 20; i++) {
            System.out.println(System.currentTimeMillis());
            Thread.sleep(3000);
        }
    }

}
