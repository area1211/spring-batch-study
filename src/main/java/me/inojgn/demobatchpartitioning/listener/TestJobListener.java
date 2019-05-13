package me.inojgn.demobatchpartitioning.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@Component
public class TestJobListener implements JobExecutionListener {

    private long start;
    private long end;

    @Override
    public void beforeJob(JobExecution jobExecution) {
        start = System.currentTimeMillis();
        log.info("================== beforeJob ==================");
        log.info("================== {} ==================", LocalDateTime.now());
        log.info("================== beforeJob ==================");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        end = System.currentTimeMillis();
        log.info("================== afterJob ==================");
        log.info("================== {} ==================", LocalDateTime.now());
        log.info("================== {} ==================", end - start);
        log.info("================== afterJob ==================");
    }
}
