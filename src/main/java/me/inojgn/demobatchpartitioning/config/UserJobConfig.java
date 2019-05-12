package me.inojgn.demobatchpartitioning.config;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.inojgn.demobatchpartitioning.RangePartitioner;
import me.inojgn.demobatchpartitioning.domain.user.User;
import me.inojgn.demobatchpartitioning.domain.user.UserRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import javax.persistence.EntityManagerFactory;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
@Configuration
@EnableBatchProcessing
public class UserJobConfig {

    private static final int CHUNK_SIZE = 10;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;
    private final UserRepository userRepository;


    @Bean
    public Job inactiveUserJob(Step partitionerStep) {
        return jobBuilderFactory.get("inactiveUserJob")
//                .preventRestart()
                .start(partitionerStep)
                .build();
    }

    @Bean
    public Job testUserJob(Step inactiveJobStep) {
        return jobBuilderFactory.get("testUserJob")
                .start(inactiveJobStep)
                .build();
    }

    @Bean//  JpaPagingItemReader<User> inactiveUserJpaReader
    public Step inactiveJobStep(ListItemReader<User> inactiveUserListReader, ItemProcessor<User, User> inactiveUserProcessor) {
        return stepBuilderFactory.get("inactiveUserStep")
                .<User, User>chunk(CHUNK_SIZE)
                .reader(inactiveUserListReader)
                .processor(inactiveUserProcessor)
                .writer(inactiveUserWriter())
                .build();
    }


    @Bean
    @JobScope
    public Step partitionerStep(Step inactiveJobStep) {
        return stepBuilderFactory.get("partitionerStep")
                .partitioner("partitionerStep", new RangePartitioner())
                .gridSize(10)
                .step(inactiveJobStep)
                .taskExecutor(taskExecutor())
                .build();
    }


    @Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor("Batch_Task");
    }


    // reader, processor, writer
    @Bean(destroyMethod = "")
    @StepScope
    public JpaPagingItemReader<User> inactiveUserJpaReader(@Value("#{stepExecutionContext[toId]}") String toId, @Value("#{stepExecutionContext[toId]}") String fromId) {
        Map<String, Object> map = new HashMap<>();
        map.put("toId", toId);
        map.put("fromId", fromId);

        return new JpaPagingItemReaderBuilder<User>()
                .name("PartitioningBatch_Reader")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(CHUNK_SIZE)
                .queryString("select u from User as u where id >= :fromId and id <= :toId")
                .parameterValues(map)
                .build();
    }

    @Bean
    @StepScope
    public ListItemReader<User> inactiveUserListReader(@Value("#{stepExecutionContext[toId]}") int toId, @Value("#{stepExecutionContext[fromId]}") int fromId) {

        log.info("=======================");
        log.info("fromId: {} toId: {}", fromId, toId);
        log.info("=======================");
        List<User> usersById = userRepository.findUsersById((long) fromId, (long) toId);
        return new ListItemReader<>(usersById);

    }

    @Bean // @Value("#{stepExecutionContext[name]}") String threadName
    @StepScope
    public ItemProcessor<User, User> inactiveUserProcessor(@Value("#{stepExecutionContext[name]}") String threadName) {
        return user -> {
            log.info("{} processing : {}", threadName, user.getIdx());
            Thread.sleep(1000l);
            user.setUpdatedDate(LocalDateTime.now());
            return user;
        };
    }

    // writer
    @Bean
    public JpaItemWriter<User> inactiveUserWriter() {
        JpaItemWriter<User> jpaItemWriter = new JpaItemWriter<>();
        jpaItemWriter.setEntityManagerFactory(entityManagerFactory);
        return jpaItemWriter;
    }
}
