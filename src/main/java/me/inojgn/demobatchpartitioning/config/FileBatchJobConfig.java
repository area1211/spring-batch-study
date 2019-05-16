package me.inojgn.demobatchpartitioning.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.inojgn.demobatchpartitioning.domain.QueryKeyword;
import me.inojgn.demobatchpartitioning.domain.sales.SalesRecord;
import me.inojgn.demobatchpartitioning.listener.TestJobListener;
import me.inojgn.demobatchpartitioning.processor.GroupProcessor;
import me.inojgn.demobatchpartitioning.reader.GroupReader;
import me.inojgn.demobatchpartitioning.writer.ConsoleItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.MultiResourceItemWriter;
import org.springframework.batch.item.file.ResourceSuffixCreator;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.task.TaskExecutor;

import javax.persistence.EntityManagerFactory;
import java.util.List;
import java.util.Map;

@Slf4j
@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class FileBatchJobConfig {
    private static final int SPLIT_SIZE = 5000;
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;
    private final TaskExecutor taskExecutor;

    private final ResourceLoader resourceLoader;
    private final GroupReader groupReader;
    private final GroupProcessor groupProcessor;

    private static final String[] HEADER = {"ContextNum", "Keyword", "Context"};
//    private static final String[] HEADER = {"Region", "Country", "Item_Type", "Sales_Channel", "Order_Priority", "Order_Date", "Order_ID", "Ship_Date", "Units_Sold", "Unit_Price", "Unit_Cost", "Total_Revenue", "Total_Cost", "Total_Profit"};


    @Bean // @Value("#{jobParameters[inputFilePath]}") String inputFilePath
    public Job readCSVFilesJob(TestJobListener testJobListener) {
        return jobBuilderFactory
                .get("readCSVFilesJob")
                .incrementer(new RunIdIncrementer()) // Entity 타입의 Id 어노테이션과 함께 GenerationType.IDENTITY 때문에 필요 없는 것 같은데..?(뇌피셜)
                .listener(testJobListener)
//                .start(step1(null))
                .start(groupingTestStep())
                .build();
    }

    @Bean
    public Step splitMultiResourceStep() {
        return stepBuilderFactory.get("splitMultiResourceStep")
                .<SalesRecord, SalesRecord>chunk(SPLIT_SIZE)
                .reader(reader(null))
                .writer(multiResourceItemWriter())
                .build();
    }

    @Bean
    public Step step1(TaskExecutor threadPoolTaskExecutor) {
        return stepBuilderFactory.get("step1")
                .<QueryKeyword, QueryKeyword>chunk(100)
                .reader(queryKeywordReader(null))
                .writer(queryKeywordJpaItemWriter())
                .taskExecutor(threadPoolTaskExecutor)
//                .throttleLimit(10)
                .build();
    }

    @Bean
    public Step groupingTestStep() {
        return stepBuilderFactory.get("groupingTestStep")
                .<List<QueryKeyword>, List<QueryKeyword>>chunk(10)
                .reader(groupReader)
                .processor(groupProcessor)
                .writer(consoleItemWriter())
                .build();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Bean
    @StepScope
    public FlatFileItemReader<QueryKeyword> queryKeywordReader(@Value("#{jobParameters[inputFilePath]}") String inputFilePath)//
    {
        FlatFileItemReader<QueryKeyword> reader = new FlatFileItemReader<>();

        reader.setResource(new FileSystemResource(inputFilePath));
        reader.setLineMapper(new DefaultLineMapper() {
            {
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames(HEADER); // "id", "firstName", "lastName"
                    }
                });

                setFieldSetMapper(new BeanWrapperFieldSetMapper<QueryKeyword>() {
                    {
                        setTargetType(QueryKeyword.class);
                    }
                });
            }
        });
        return reader;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Bean
    @StepScope
    public FlatFileItemReader<SalesRecord> reader(@Value("#{jobParameters[inputFilePath]}") String inputFilePath)//
    {
//        String inputFilePath = "input/10000 Sales Records.csv";
        //Create reader instance
        FlatFileItemReader<SalesRecord> reader = new FlatFileItemReader<>();

        //Set input file location
        reader.setResource(new FileSystemResource(inputFilePath));
//        reader.setResource(resourceLoader.getResource(inputFilePath));


        //Set number of lines to skips. Use it if file has header rows.
//        reader.setLinesToSkip(1);

        //Configure how each line will be parsed and mapped to different values
        reader.setLineMapper(new DefaultLineMapper() {
            {
                //3 columns in each row
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames(HEADER); // "id", "firstName", "lastName"
                    }
                });
                //Set values in SalesRecord class
                setFieldSetMapper(new BeanWrapperFieldSetMapper<SalesRecord>() {
                    {
                        setTargetType(SalesRecord.class);
                    }
                });
            }
        });
        return reader;
    }

    @Bean
    public ConsoleItemWriter<List<QueryKeyword>> consoleItemWriter() {
        return new ConsoleItemWriter<>();
    }

    @Bean
    public JpaItemWriter<QueryKeyword> queryKeywordJpaItemWriter() {
        JpaItemWriter<QueryKeyword> jpaItemWriter = new JpaItemWriter<>();
        jpaItemWriter.setEntityManagerFactory(entityManagerFactory);
        return jpaItemWriter;
    }

    @Bean
    public JpaItemWriter<SalesRecord> testJpaItemWriter() {
        JpaItemWriter<SalesRecord> jpaItemWriter = new JpaItemWriter<>();
        jpaItemWriter.setEntityManagerFactory(entityManagerFactory);
        return jpaItemWriter;
    }


    @Bean
    public FlatFileItemWriter<SalesRecord> flatFileItemWriter() {
        FlatFileItemWriter<SalesRecord> flatFileItemWriter = new FlatFileItemWriter<>();

        flatFileItemWriter.setLineAggregator(new DelimitedLineAggregator<SalesRecord>() {
            {
                setFieldExtractor(new BeanWrapperFieldExtractor() {
                    {
                        setNames(new String[]{"Region", "Country", "ItemType", "SalesChannel", "OrderPriority", "OrderDate", "OrderId", "ShipDate", "UnitsSold", "UnitPrice", "UnitCost", "TotalRevenue", "TotalCost", "TotalProfit"});
                    }
                });
            }
        });

        return flatFileItemWriter;
    }

    @Bean
    public MultiResourceItemWriter multiResourceItemWriter() {
        MultiResourceItemWriter multiResourceItemWriter = new MultiResourceItemWriter();
        multiResourceItemWriter.setDelegate(flatFileItemWriter());
        multiResourceItemWriter.setResource(new FileSystemResource("input/split/TestData_"));
        multiResourceItemWriter.setItemCountLimitPerResource(SPLIT_SIZE);
        multiResourceItemWriter.setResourceSuffixCreator(index -> index + ".csv");


        return multiResourceItemWriter;
    }
}
