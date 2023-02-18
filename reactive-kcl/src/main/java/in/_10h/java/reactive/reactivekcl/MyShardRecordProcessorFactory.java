package in._10h.java.reactive.reactivekcl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

@Component
public class MyShardRecordProcessorFactory implements ShardRecordProcessorFactory {

    private final ReactiveShardRecordProcessorFactory recordProcessorFactory;

    @Autowired
    public MyShardRecordProcessorFactory(
            final ReactiveShardRecordProcessorFactory recordProcessorFactory
    ) {
        this.recordProcessorFactory = recordProcessorFactory;
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor() {
        return new MyShardRecordProcessor(this.recordProcessorFactory.createRecordProcessor());
    }
}
