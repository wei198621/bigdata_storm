package com.tiza.leo.bigdata.storm.test01Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

/**
 * @author leowei
 * @date 2021/4/8  - 22:03
 */
public class RandomStringSpoutTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void open() {
        System.out.println(ThreadLocalRandom.current().nextInt(0, 5));
        System.out.println(ThreadLocalRandom.current().nextInt(0, 5));
        System.out.println(ThreadLocalRandom.current().nextInt(0, 5));
        System.out.println(ThreadLocalRandom.current().nextInt(0, 5));
        System.out.println(ThreadLocalRandom.current().nextInt(0, 5));
        System.out.println(ThreadLocalRandom.current().nextInt(0, 5));
    }
}