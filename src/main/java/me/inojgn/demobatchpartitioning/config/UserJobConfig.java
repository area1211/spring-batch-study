package me.inojgn.demobatchpartitioning.config;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.inojgn.demobatchpartitioning.partitioner.RangePartitioner;
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

@Slf4j
@RequiredArgsConstructor
@Configuration
@EnableBatchProcessing
public class UserJobConfig {

    private static final int CHUNK_SIZE = 20;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;
    private final UserRepository userRepository;

    private final RangePartitioner rangePartitioner;

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

    @Bean//  JpaPagingItemReader<User> inactiveUserJpaReader , ListItemReader<User> inactiveUserListReader
    public Step inactiveJobStep() {
        return stepBuilderFactory.get("inactiveUserStep")
                // chunk 단위로 reader에서 processor로 전달된다.
                .<User, User>chunk(CHUNK_SIZE)
                .reader(inactiveUserJpaReader(0, 0))
                .processor(inactiveUserProcessor(null))
                .writer(inactiveUserWriter())
                .allowStartIfComplete(true)
                .build();
    }


    @Bean
    @JobScope
    public Step partitionerStep() {
        return stepBuilderFactory.get("partitionerStep")
                .partitioner("partitionerStep", rangePartitioner)
                .gridSize(5)
                .step(inactiveJobStep())
                .taskExecutor(taskExecutor())
                .allowStartIfComplete(true)// ??????/
                .build();
    }




    // reader, processor, writer
    @Bean(destroyMethod = "")
    @StepScope
    public JpaPagingItemReader<User> inactiveUserJpaReader(@Value("#{stepExecutionContext[toId]}") int toId, @Value("#{stepExecutionContext[fromId]}") int fromId) {
        log.info("========JpaPagingItemReader=========");
        log.info("fromId: {} toId: {}", fromId, toId);
        log.info("========JpaPagingItemReader=========");
        int pageSize = 20;

        return new JpaPagingItemReaderBuilder<User>()
                .name("PartitioningBatch_Reader")
                .entityManagerFactory(entityManagerFactory)
                .saveState(false) // ???
//                .pageSize(CHUNK_SIZE)
                .queryString("select u from User as u where u.idx >= :fromId and u.idx <= :toId order by idx") //  limit :fromId offset :toId , where u.idx >= :fromId and u.idx <= :toId
                .parameterValues(new HashMap<String, Object>(){
                    {
                        put("fromId", (long)fromId);
                        put("toId", (long)toId);
                    }
                })
                .build();
    }

    @Bean
    @StepScope
    public ListItemReader<User> inactiveUserListReader(@Value("#{stepExecutionContext[toId]}") int toId, @Value("#{stepExecutionContext[fromId]}") int fromId) {

        log.info("========ListItemReader=======");
        log.info("fromId: {} toId: {}", fromId, toId);
        log.info("========ListItemReader=======");
        List<User> usersById = userRepository.findUsersById((long) fromId, (long) toId);
        return new ListItemReader<>(usersById);

    }

    @Bean // @Value("#{stepExecutionContext[name]}") String threadName
    @StepScope
    public ItemProcessor<User, User> inactiveUserProcessor(@Value("#{stepExecutionContext[name]}") String threadName) {
        return user -> {
            log.info("{} processing : {}", threadName, user.getIdx());
            Thread.sleep(1000l); // 아이템 하나 당 1초의 시간이 걸린다고 가정한다.
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


//    @Value("${etl.factiva.partition.cores}")
    private int threadPoolSize = 0;

    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor("Batch_Task");

        taskExecutor.setConcurrencyLimit(determineWorkerThreads());
        return taskExecutor;
    }

//    @Bean
//    public TaskExecutor taskExecutor() {
//        return new SimpleAsyncTaskExecutor("Batch_Task");
//    }


    // threadPoolSize is a configuration parameter for the job
    private int determineWorkerThreads() {
        if (threadPoolSize == 0) {
            threadPoolSize = Runtime.getRuntime().availableProcessors();
        }
        log.info("=========determineWorkerThreads=========");
        log.info("threadPoolSize : {}", threadPoolSize);
        log.info("========================================");
        return threadPoolSize;
    }
}
