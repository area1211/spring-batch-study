package me.inojgn.demobatchpartitioning.partitioner;

import lombok.extern.slf4j.Slf4j;
import me.inojgn.demobatchpartitioning.domain.user.User;
import me.inojgn.demobatchpartitioning.domain.user.UserRepository;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class RangePartitioner implements Partitioner {

    @Autowired
    private UserRepository userRepository;

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        List<User> all = userRepository.findAll();
        int tableSize = all.size();

        log.info("===============partition===============");
        log.info("      tableSize : {}", tableSize);

        Map<String, ExecutionContext> map = new HashMap<>(gridSize);
        int range = (tableSize - 1) / gridSize + 1;
        int fromId = 1;
        int toId = range;

        log.info("      range : {}", range);

        for (int i = 1; i <= gridSize; i++) {
            ExecutionContext context = new ExecutionContext();

            log.info("\nStarting : Thread" + i);
            log.info("fromId : " + fromId);
            log.info("toId : " + toId);

            context.putInt("fromId", fromId);
            context.putInt("toId", toId);
            context.putString("name", "Thread" + i);

            map.put("partition" + i, context); // step execution 의 이름과 context 를 쌍으로 넣어준다.
            fromId = toId + 1;
            toId += range;

        }
        log.info("===============partition===============");

        return map;
    }
}
