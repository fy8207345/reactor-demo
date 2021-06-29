package org.fy.reactor;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ConcurrentHashMapTests {

    @Test
    void name() {

        ConcurrentHashMap<Integer, Integer> map = new ConcurrentHashMap<>();
        for(int i=0;i<20;i++){
            map.put(0xff0000+i, i);
        }
    }
}
