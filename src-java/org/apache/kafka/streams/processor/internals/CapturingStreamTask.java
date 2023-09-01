package org.apache.kafka.streams.processor.internals;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.streams.TopologyConfig.TaskConfig;
import org.apache.kafka.streams.TopologyConfig;


import clojure.lang.IFn;

public class CapturingStreamTask extends StreamTask {

	private final StreamTask delegate;
	private final IFn capture;

  private static StreamsConfig dumbConfigStreams() {
    HashMap<String, String> m = new HashMap<>();
    m.put("application.id", "");
    m.put("bootstrap.servers", "");
    return new ClientUtils.QuietStreamsConfig(m);
  }

	private static TaskConfig dumbConfigTask() {
      return new TopologyConfig(dumbConfigStreams()).getTaskConfig();
	}

	private static StreamsMetricsImpl dumbMetrics(Time t) {
		return new StreamsMetricsImpl(
				new Metrics(t),
				"",
				"",
				t
				);
	}

	public CapturingStreamTask(StreamTask delegate, IFn privateFieldGetter, IFn capture) {
		// don't talk to me about this
		super(delegate.id(),
				delegate.inputPartitions(),
				delegate.topology,
				(Consumer<byte[], byte[]>) privateFieldGetter.invoke(delegate, "mainConsumer"),
				dumbConfigTask(),
				dumbMetrics((Time) privateFieldGetter.invoke(delegate, "time")),
				delegate.stateDirectory,
				null,
				(Time) privateFieldGetter.invoke(delegate, "time"),
				delegate.stateMgr,
				(RecordCollector) privateFieldGetter.invoke(delegate, "recordCollector"),
				new ProcessorContextImpl(
						delegate.id(),
						dumbConfigStreams(), delegate.stateMgr,
						dumbMetrics((Time) privateFieldGetter.invoke(delegate, "time")),
						null) {
					@Override
					public void transitionToActive(StreamTask streamTask, RecordCollector recordCollector, ThreadCache newCache) { }
				},
        new LogContext( "streams-task"));
		this.delegate = delegate;
		this.capture = capture;
	}

	public void addRecords(TopicPartition partition, Iterable<ConsumerRecord<byte[], byte[]>> records) {
		for (ConsumerRecord<byte[], byte[]> record : records) {
			capture.invoke(delegate, partition, record);
		}
	}

	@Override
	public boolean isActive() {
		return delegate.isActive();
	}

	@Override
	public void initializeIfNeeded() {
		delegate.initializeIfNeeded();
	}

	@Override
	public void addPartitionsForOffsetReset(Set<TopicPartition> partitionsForOffsetReset) {
		delegate.addPartitionsForOffsetReset(partitionsForOffsetReset);
	}

	@Override
	public void completeRestoration(java.util.function.Consumer<Set<TopicPartition>> offsetResetter) {
		delegate.completeRestoration(offsetResetter);
	}

	@Override
	public void suspend() {
		delegate.suspend();
	}

	@Override
	public void resume() {
		delegate.resume();
	}

	@Override
	public Map<TopicPartition, OffsetAndMetadata> prepareCommit() {
		return delegate.prepareCommit();
	}

	@Override
	public void postCommit(boolean enforceCheckpoint) {
		delegate.postCommit(enforceCheckpoint);
	}

	@Override
	public void closeClean() {
		delegate.closeClean();
	}

	@Override
	public void closeDirty() {
		delegate.closeDirty();
	}

	@Override
	public void updateInputPartitions(Set<TopicPartition> topicPartitions, Map<String, List<String>> allTopologyNodesToSourceTopics) {
		delegate.updateInputPartitions(topicPartitions, allTopologyNodesToSourceTopics);
	}

	@Override
	public boolean isProcessable(long wallClockTime) {
		return delegate.isProcessable(wallClockTime);
	}

	@Override
	public boolean process(long wallClockTime) {
		return delegate.process(wallClockTime);
	}

	@Override
	public void recordProcessBatchTime(long processBatchTime) {
		delegate.recordProcessBatchTime(processBatchTime);
	}

	@Override
	public void recordProcessTimeRatioAndBufferSize(long allTaskProcessMs, long now) {
		delegate.recordProcessTimeRatioAndBufferSize(allTaskProcessMs, now);
	}

	@Override
	public void punctuate(ProcessorNode<?, ?, ?, ?> node, long timestamp, PunctuationType type, Punctuator punctuator) {
		delegate.punctuate(node, timestamp, type, punctuator);
	}

	@Override
	public Map<TopicPartition, Long> purgeableOffsets() {
		return delegate.purgeableOffsets();
	}

	@Override
	public Cancellable schedule(long interval, PunctuationType type, Punctuator punctuator) {
		return delegate.schedule(interval, type, punctuator);
	}

	@Override
	public boolean maybePunctuateStreamTime() {
		return delegate.maybePunctuateStreamTime();
	}

	@Override
	public boolean maybePunctuateSystemTime() {
		return delegate.maybePunctuateSystemTime();
	}

	@Override
	public boolean commitRequested() {
		return delegate.commitRequested();
	}

	@Override
	public InternalProcessorContext processorContext() {
		return delegate.processorContext();
	}

	@Override
	public String toString() {
		return delegate.toString();
	}

	@Override
	public String toString(String indent) {
		return delegate.toString(indent);
	}

	@Override
	public boolean commitNeeded() {
		return delegate.commitNeeded();
	}

	@Override
	public Map<TopicPartition, Long> changelogOffsets() {
		return delegate.changelogOffsets();
	}

	@Override
	public boolean hasRecordsQueued() {
		return delegate.hasRecordsQueued();
	}

	@Override
	public TaskId id() {
		return delegate.id();
	}

	@Override
	public void markChangelogAsCorrupted(Collection<TopicPartition> partitions) {
		delegate.markChangelogAsCorrupted(partitions);
	}

	@Override
	public StateStore getStore(String name) {
		return delegate.getStore(name);
	}

	@Override
	public void revive() {
		delegate.revive();
	}

	@Override
	public void maybeInitTaskTimeoutOrThrow(long currentWallClockMs, Exception cause) {
		delegate.maybeInitTaskTimeoutOrThrow(currentWallClockMs, cause);
	}

	@Override
	public void clearTaskTimeout() {
		delegate.clearTaskTimeout();
	}

	public boolean needsInitializationOrRestoration() {
		return delegate.needsInitializationOrRestoration();
	}
}
