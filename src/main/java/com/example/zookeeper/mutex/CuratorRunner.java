package com.example.zookeeper.mutex;

import com.example.zookeeper.job.JobRunner;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.TimeUnit;

import static com.example.zookeeper.Profiles.CURATOR;

@Component
@Profile(CURATOR)
public class CuratorRunner implements MutexRunner {

    private static final Logger log = LoggerFactory.getLogger(CuratorRunner.class);

    private final CuratorFramework curatorFramework;
    private final InterProcessSemaphoreMutex mutex;
    private final JobRunner jobRunner;

    @PostConstruct
    public void startUp() {
        curatorFramework.start();
    }

    public CuratorRunner(CuratorFramework curatorFramework, JobRunner jobRunner) {
        this.curatorFramework = curatorFramework;
        this.jobRunner = jobRunner;
        this.mutex = new InterProcessSemaphoreMutex(curatorFramework, "/mutex");
    }

    @Override
    public void run() {
        try {
            mutex.acquire(1, TimeUnit.MILLISECONDS);
            if (mutex.isAcquiredInThisProcess()) {
                jobRunner.run();
            } else {
                log.info("Job is already running by another node");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }  catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (mutex.isAcquiredInThisProcess()) {
                    mutex.release();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("Error occurred", e);
            }
        }
    }
}
