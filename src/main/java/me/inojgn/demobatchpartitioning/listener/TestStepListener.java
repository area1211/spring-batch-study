package me.inojgn.demobatchpartitioning.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;

@Slf4j
public class TestStepListener {

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {

    }

    @AfterStep
    public void afterStep(StepExecution stepExecution) {

    }

}
