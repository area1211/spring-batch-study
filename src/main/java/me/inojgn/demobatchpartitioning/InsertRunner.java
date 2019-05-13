package me.inojgn.demobatchpartitioning;

import me.inojgn.demobatchpartitioning.domain.user.User;
import me.inojgn.demobatchpartitioning.domain.user.UserRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.IntStream;

@Component
public class InsertRunner implements ApplicationRunner {

    @Autowired
    Job inactiveUserJob;

    @Autowired
    Job readCSVFilesJob;

    @Autowired
    JobLauncher jobLauncher;

    @Override
    public void run(ApplicationArguments args) throws Exception {

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("JobID", String.valueOf(System.currentTimeMillis()))
                .addString("inputFilePath", "input/inputData.csv")
                .toJobParameters();

        jobLauncher.run(readCSVFilesJob, jobParameters);

//        IntStream.rangeClosed(101, 238).forEach(index ->
//                userRepository.save(User.builder()
//                        .name("kinguser_"+index)
//                        .password("test_"+index)
//                        .email("user_"+index+"@gmail.com")
//                        .createdDate(LocalDateTime.now())
//                        .build()));
    }
}
