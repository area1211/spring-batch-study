package me.inojgn.demobatchpartitioning;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

public class RangePartitioner implements Partitioner {


    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {

        Map<String, ExecutionContext> map = new HashMap<>(gridSize);
        int range = 10;
        int fromId = 1;
        int toId = range;

        for (int i = 1; i <= gridSize; i++) {
            ExecutionContext context = new ExecutionContext();

            System.out.println("\nStarting : Thread" + i);
            System.out.println("fromId : " + fromId);
            System.out.println("toId : " +toId);

            context.putInt("fromId", fromId);
            context.putInt("toId", toId);
            context.putString("name", "Thread" + i);

            map.put("partition"+i,context);
            fromId = toId + 1;
            toId += range;

        }

        return map;
    }
}
