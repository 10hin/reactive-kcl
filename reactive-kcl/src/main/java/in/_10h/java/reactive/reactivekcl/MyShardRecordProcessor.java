package in._10h.java.reactive.reactivekcl;

import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.util.List;

public class MyShardRecordProcessor implements ShardRecordProcessor {

    private final ReactiveShardRecordProcessor reactiveShardRecordProcessor;
    private FluxSink<KinesisClientRecord> emitter;

    public MyShardRecordProcessor(
            final ReactiveShardRecordProcessor reactiveShardRecordProcessor
    ) {
        this.reactiveShardRecordProcessor = reactiveShardRecordProcessor;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {

        this.reactiveShardRecordProcessor.initialize(Flux.push((sink) -> { this.emitter = sink; }))
                .subscribe();

    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        final List<KinesisClientRecord> records = processRecordsInput.records();
        for (final var record : records) {
            this.emitter.next(record);
        }
        try {
            processRecordsInput.checkpointer().checkpoint();
        } catch (InvalidStateException e) {
            throw new RuntimeException(e);
        } catch (ShutdownException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        this.emitter.complete();
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        try {
            shardEndedInput.checkpointer().checkpoint();
        } catch (InvalidStateException e) {
            throw new RuntimeException(e);
        } catch (ShutdownException e) {
            throw new RuntimeException(e);
        }
        this.emitter.complete();
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        try {
            shutdownRequestedInput.checkpointer().checkpoint();
        } catch (InvalidStateException e) {
            throw new RuntimeException(e);
        } catch (ShutdownException e) {
            throw new RuntimeException(e);
        }
        this.emitter.complete();
    }
}
