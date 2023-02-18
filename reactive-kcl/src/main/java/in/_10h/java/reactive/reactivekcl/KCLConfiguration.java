package in._10h.java.reactive.reactivekcl;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.nio.netty.Http2Configuration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy;
import software.amazon.kinesis.processor.MultiStreamTracker;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.util.List;
import java.util.UUID;

@Configuration
public class KCLConfiguration {
    private Region region;
    /** also used as name of lease table (DynamoDB Table) */
    private String applicationName;

    private String streamName;

    private String workerIdentifier;

    @Value("${aws.region:ap-northrast-1}")
    public void setRegion(final String region) {
        this.region = Region.of(region);
    }

    @Value("${kcl.applicationName:spring.application.name}")
    public void setApplicationName(final String applicationName) {
        this.applicationName = applicationName;
    }

    @Value("${kcl.streamName}")
    public void setStreamName(final String streamName) {
        this.streamName = streamName;
    }

    @Value("${kcl.workerIdentifier:}")
    public void setWorkerIdentifier(final String workerIdentifier) {
        if (workerIdentifier.isBlank()) {
            this.workerIdentifier = UUID.randomUUID().toString();
        }
        this.workerIdentifier = workerIdentifier;
    }

    @Bean
    public MultiStreamTracker multiStreamTracker() {
        return new FixedSingleStreamTracker(this.streamName);
    }

    @Bean
    public KinesisAsyncClient kinesisClient() {

        return KinesisClientUtil.createKinesisAsyncClient(
                KinesisAsyncClient
                        .builder()
                        .region(this.region)
        );

    }

    @Bean
    public DynamoDbAsyncClient dynamoDBClient() {

        return DynamoDbAsyncClient.builder().region(this.region)
                .httpClientBuilder(
                        NettyNioAsyncHttpClient
                                .builder()
                                .maxConcurrency(Integer.MAX_VALUE)
                                .protocol(Protocol.HTTP2)
                )
                .build();

    }

    @Bean
    public CloudWatchAsyncClient cloudWatchClient() {

        return CloudWatchAsyncClient.builder().region(this.region)
                .httpClientBuilder(
                        NettyNioAsyncHttpClient
                                .builder()
                                .maxConcurrency(Integer.MAX_VALUE)
                                .protocol(Protocol.HTTP2)
                )
                .build();

    }

    @Bean
    public Scheduler kclScheduler(
            final MultiStreamTracker multiStreamTracker,
            final KinesisAsyncClient kinesisClient,
            final DynamoDbAsyncClient dynamoDBClient,
            final CloudWatchAsyncClient cloudWatchClient,
            final ShardRecordProcessorFactory shardRecordProcessorFactory
    ) {

        final ConfigsBuilder configsBuilder = new ConfigsBuilder(
                multiStreamTracker,
                this.applicationName,
                kinesisClient,
                dynamoDBClient,
                cloudWatchClient,
                this.workerIdentifier,
                shardRecordProcessorFactory
        );
        final var scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig().retrievalSpecificConfig(new PollingConfig("", kinesisClient))
        );

        new Thread(scheduler).start();

        return scheduler;

    }

    private static class FixedSingleStreamTracker implements MultiStreamTracker {
        private final static FormerStreamsLeasesDeletionStrategy NO_LEASE_DELETION_STRATEGY = new FormerStreamsLeasesDeletionStrategy.NoLeaseDeletionStrategy();
        private final List<StreamConfig> streamConfigs;
        public FixedSingleStreamTracker(final String streamName) {
            final var streamId = StreamIdentifier.singleStreamInstance(streamName);
            final var initialPosition = InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);
            this.streamConfigs = List.of(new StreamConfig(streamId, initialPosition));
        }

        @Override
        public List<StreamConfig> streamConfigList() {
            return this.streamConfigs;
        }

        @Override
        public FormerStreamsLeasesDeletionStrategy formerStreamsLeasesDeletionStrategy() {
            return FixedSingleStreamTracker.NO_LEASE_DELETION_STRATEGY;
        }

    }
}
