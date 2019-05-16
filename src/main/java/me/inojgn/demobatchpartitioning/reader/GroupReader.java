package me.inojgn.demobatchpartitioning.reader;

import apple.laf.JRSUIConstants;
import lombok.RequiredArgsConstructor;
import me.inojgn.demobatchpartitioning.domain.QueryKeyword;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.*;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.support.SingleItemPeekableItemReader;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;


import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Component
@RequiredArgsConstructor
@EnableBatchProcessing
public class GroupReader implements ItemReader<List<QueryKeyword>>, ItemStream {

    private SingleItemPeekableItemReader<QueryKeyword> reader;
    private final ItemReader<QueryKeyword> peekReaderDelegate;
    private final FlatFileItemReader<QueryKeyword> queryKeywordReader;

    private QueryKeyword peekedItem;

    @PostConstruct
    public void init() {
        Assert.notNull(queryKeywordReader, "The 'itemReader' may not be null");
        this.reader = new SingleItemPeekableItemReader<>();
        this.reader.setDelegate(queryKeywordReader);

    }




    @Override
    public List<QueryKeyword> read() throws Exception{
        State state = State.NEW;
        List<QueryKeyword> group = null;
        QueryKeyword item = null;

        while (state != State.COMPLETE) {
            item = reader.read();

            switch (state) {
                case NEW: {
                    if (item == null) {
                        state = State.COMPLETE;
                        break;
                    }

                    group = new ArrayList<>();
                    group.add(item);
                    state = State.READING;

                    QueryKeyword nextItem = reader.peek();
                    if (isItAKeyChange(item, nextItem)) {
                        state = State.COMPLETE;
                    }
                    break;
                }
                case READING: {
                    group.add(item);

                    QueryKeyword nextItem = reader.peek();
                    if (isItAKeyChange(item, nextItem)) {
                        state = State.COMPLETE;
                    }
                    break;
                }
                default: {
                    throw new org.springframework.expression.ParseException(123, "ParsingError: Reader is in an invalid state");
                }
            }
        }
        return group;
    }

    private QueryKeyword peekEntry() throws Exception {
        if (peekedItem != null)
            return peekedItem;
        else
            return reader.read();
    }

    private boolean isItAKeyChange(QueryKeyword item, QueryKeyword nextItem) {
        if(nextItem == null) {
            return true;
        }
        return !item.getContextNum().equals(nextItem.getContextNum());
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        queryKeywordReader.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        queryKeywordReader.update(executionContext);
    }

    @Override
    public void close() throws ItemStreamException {
        queryKeywordReader.close();
    }

    enum State {NEW, COMPLETE, READING}
}
