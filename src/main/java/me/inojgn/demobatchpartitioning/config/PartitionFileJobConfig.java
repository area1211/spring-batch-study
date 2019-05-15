package me.inojgn.demobatchpartitioning.config;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.inojgn.demobatchpartitioning.domain.sales.SalesRecord;
import me.inojgn.demobatchpartitioning.listener.TestJobListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.persistence.EntityManagerFactory;
import java.io.IOException;
import java.net.MalformedURLException;


@Slf4j
@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class PartitionFileJobConfig {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    private final Step splitMultiResourceStep;
    private final JpaItemWriter<SalesRecord> testJpaItemWriter;


    @Bean
    public Job readMultipleFileWriteDBJob(TestJobListener testJobListener) throws MalformedURLException {
        return jobBuilderFactory
                .get("readMultipleFileWriteDBJob")
//                .incrementer(new RunIdIncrementer()) // Entity 타입의 Id 어노테이션과 함께 GenerationType.IDENTITY 때문에 필요 없는 것 같은데..?(뇌피셜)
                .listener(testJobListener)
                .start(splitMultiResourceStep)
                .next(masterStep())
                .build();
    }

    @Bean("partitioner")
    @StepScope
    public Partitioner partitioner() {
        log.info("In Partitioner");
        MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = null;
        try {
//            resources = resolver.getResources("/*.csv"); //new FileSystemResource(inputFilePath)
//            resources = resolver.getResources("classpath:input/TestData_*.csv"); // classpath:mappers/embed/*.xml
            resources = resolver.getResources("file:input/split/TestData_*.csv"); // classpath:mappers/embed/*.xml, /Users/jeong-inho/IdeaProjects/hackday/demobatchpartitioning/
        } catch (IOException e) {
            e.printStackTrace();
        }
        partitioner.setResources(resources);
        partitioner.partition(20);
        return partitioner;
    }

    @Bean
    @Qualifier("masterStep")
    public Step masterStep() throws MalformedURLException {
        return stepBuilderFactory.get("masterStep")
                .partitioner("stepTest", partitioner())
//                .gridSize(20)
                .step(stepTest())
                .taskExecutor(threadPoolTaskExecutor())
                .build();
    }

    @Bean
    public Step stepTest() throws MalformedURLException {
        return stepBuilderFactory.get("stepTest")
                .<SalesRecord, SalesRecord>chunk(500)
                .reader(personItemReader(null))
                .writer(testJpaItemWriter)
                .build();
    }

    @Bean
    public ThreadPoolTaskExecutor threadPoolTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        // corePoolSize와 maxPoolSize를 같게 하면 결국 고정 크기 스레드 풀을 생성하게 된다.
        int size = 20;
        taskExecutor.setMaxPoolSize(size); // 스레드풀에서 관리하는 최대 스레드 수
        taskExecutor.setCorePoolSize(size); // 스레드가 증가한 후 사용되지 않은 스레드를 스레드 풀에서 제거할 때 최소한으로 유지해야할 수
        taskExecutor.setQueueCapacity(size);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }

    @Bean
    @StepScope
    @Qualifier("personItemReader")
    public FlatFileItemReader<SalesRecord> personItemReader(@Value("#{stepExecutionContext['fileName']}") String filename) throws MalformedURLException {

        log.info("In Reader");
        return new FlatFileItemReaderBuilder<SalesRecord>()
                .name("personItemReader")
                .delimited()
                .names(new String[] { "Region", "Country", "Item_Type", "Sales_Channel", "Order_Priority", "Order_Date", "Order_ID", "Ship_Date", "Units_Sold", "Unit_Price", "Unit_Cost", "Total_Revenue", "Total_Cost", "Total_Profit" })
                .fieldSetMapper(new BeanWrapperFieldSetMapper<SalesRecord>() {
                    {
                        setTargetType(SalesRecord.class);
                    }
                })
                .resource(new UrlResource(filename))
                .build();
    }


}
