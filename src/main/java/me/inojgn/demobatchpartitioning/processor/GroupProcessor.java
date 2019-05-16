package me.inojgn.demobatchpartitioning.processor;

import lombok.extern.slf4j.Slf4j;
import me.inojgn.demobatchpartitioning.domain.QueryKeyword;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.List;


@Slf4j
@Component
public class GroupProcessor implements ItemProcessor<List<QueryKeyword>, List<QueryKeyword>> {


    @Override
    public List<QueryKeyword> process(List<QueryKeyword> item) throws Exception {

        log.debug("====================================");

        log.debug("======ContextNum:{}, Size:{}========", item.get(0).getContextNum(), item.size());

        log.debug("====================================");

        return item;
    }
}
