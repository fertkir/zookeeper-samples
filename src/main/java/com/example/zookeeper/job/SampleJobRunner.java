package com.example.zookeeper.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class SampleJobRunner implements JobRunner {

    private static final Logger log = LoggerFactory.getLogger(SampleJobRunner.class);

    @Override
    public void run() {
        try {
            log.info("Started job");
            Thread.sleep(5000);
            log.info("Finished job");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
