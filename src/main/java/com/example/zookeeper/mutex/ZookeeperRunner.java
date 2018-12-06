package com.example.zookeeper.mutex;

import com.example.zookeeper.job.JobRunner;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import static com.example.zookeeper.Profiles.ZOOKEEPER;

@Component
@Profile(ZOOKEEPER)
public class ZookeeperRunner implements MutexRunner {

    private static final Logger log = LoggerFactory.getLogger(ZookeeperRunner.class);

    private final ZooKeeper zooKeeper;
    private final JobRunner jobRunner;

    public ZookeeperRunner(ZooKeeper zooKeeper, JobRunner jobRunner) {
        this.zooKeeper = zooKeeper;
        this.jobRunner = jobRunner;
    }

    @Override
    public void run() {
        try {
            zooKeeper.create("/lock", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            jobRunner.run();
            zooKeeper.delete("/lock", 0); // may not be deleted if exception occurs above
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NODEEXISTS) {
                log.info("Job is already running by another node");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
