package com.bytegriffin.get4j.core;

import java.util.List;

import com.google.common.collect.Lists;

public class Chain {

    List<Process> list = Lists.newArrayList();

    public void execute(Page page) {
        for (Process p : list) {
            p.execute(page);
        }
    }

    public Chain addProcess(Process p) {
        list.add(p);
        return this;
    }

}
