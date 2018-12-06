package com.example.zookeeper.configuration;

import com.example.zookeeper.mutex.MutexRunner;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.IOException;

import static com.example.zookeeper.Profiles.CURATOR;
import static com.example.zookeeper.Profiles.ZOOKEEPER;

@Configuration
public class JobConfiguration {

    @Autowired
    private MutexRunner runner;

    private final String connectString;

    public JobConfiguration(@Value("${zookeeper.connectString}") String connectString) {
        this.connectString = connectString;
    }

    @Scheduled(cron = "${job.cron}")
    public void run() {
        runner.run();
    }

    @Bean
    @Profile(ZOOKEEPER)
    public ZooKeeper zooKeeper() throws IOException {
        return new ZooKeeper(connectString, 30000, event -> {
            // nop
        });
    }

    @Bean
    @Profile(CURATOR)
    public CuratorFramework curatorFramework() {
        return CuratorFrameworkFactory.newClient(connectString,
                new ExponentialBackoffRetry(1000, 3));
    }
}
