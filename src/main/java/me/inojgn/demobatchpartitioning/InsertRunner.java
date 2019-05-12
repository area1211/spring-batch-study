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
    private UserRepository userRepository;

    @Autowired
    Job inactiveUserJob;

//    @Autowired
//    Job testUserJob;

    @Autowired
    JobLauncher jobLauncher;

    @Override
    public void run(ApplicationArguments args) throws Exception {
//        List<User> all = userRepository.findAll();
//        System.out.println("==================");
//        System.out.println(all.size());
//        System.out.println("==================");

//        List<User> usersById = userRepository.findUsersById(1l, 50l);
//        System.out.println(usersById.size());

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        jobLauncher.run(inactiveUserJob, jobParameters);

//        IntStream.rangeClosed(201, 301).forEach(index ->
//                userRepository.save(User.builder()
//                        .name("kinguser_"+index)
//                        .password("test_"+index)
//                        .email("user_"+index+"@gmail.com")
//                        .createdDate(LocalDateTime.now())
//                        .build()));
    }
}
