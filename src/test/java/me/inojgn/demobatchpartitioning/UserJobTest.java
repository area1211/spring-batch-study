package me.inojgn.demobatchpartitioning;

import me.inojgn.demobatchpartitioning.domain.user.User;
import me.inojgn.demobatchpartitioning.domain.user.UserRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.EmbeddedDatabaseConnection;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
//@ContextConfiguration(classes = { TestJobConfig.class, UserJobConfig.class})
@SpringBootTest
//@AutoConfigureTestDatabase(connection = EmbeddedDatabaseConnection.H2)
public class UserJobTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    UserRepository userRepository;

    @Test
    public void 파티셔닝_테스트() throws Exception {
//        IntStream.rangeClosed(1, 100).forEach(index ->
//                userRepository.save(User.builder()
//                        .name("user_"+index)
//                        .password("test_"+index)
//                        .email("user_"+index+"@gmail.com")
//                        .createdDate(LocalDateTime.now())
//                        .build()));

//        List<User> all = userRepository.findAll();
//        for (User u :
//                all) {
//            System.out.println(u.getName());
//
//        }
//        System.out.println("ListSIze : " + all.size());


        JobExecution jobExecution = jobLauncherTestUtils.launchJob();


//        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

}
