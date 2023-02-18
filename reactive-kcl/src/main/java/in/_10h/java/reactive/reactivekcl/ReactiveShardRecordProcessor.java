package in._10h.java.reactive.reactivekcl;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public interface ReactiveShardRecordProcessor {

    default Flux<Void> initialize(Flux<KinesisClientRecord> source) {
        return source.flatMap(this::processSingleRecord);
    }

    default Mono<Void> processSingleRecord(KinesisClientRecord record) {
        return Mono.empty();
    }

}
