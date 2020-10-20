package org.apache.kafka.streams.processor.internals;

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.internals.QuietStreamsConfig;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.RecordCollectorImpl;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import clojure.lang.IFn;

public class CapturingStreamTask extends StreamTask {

	private final StreamTask delegate;
	private final IFn capture;

	private static StreamsConfig dumbConfig() {
		HashMap<String, String> m = new HashMap<>();
		m.put("application.id", "");
		m.put("bootstrap.servers", "");
		return new QuietStreamsConfig(m);
	}

	public CapturingStreamTask(StreamTask delegate, IFn capture) {
		// don't talk to me about this
		super(delegate.id(),
				delegate.partitions(),
				delegate.topology(),
				null,
				null,
				dumbConfig(),
				new StreamsMetricsImpl(new Metrics(Time.SYSTEM), "", ""),
				new StateDirectory(dumbConfig(), Time.SYSTEM, false),
				null,
				null,
				new ProducerSupplier() {
					@Override
					public Producer<byte[], byte[]> get() {
						return null;
					}
				},
				new RecordCollectorImpl("", new LogContext(), new DefaultProductionExceptionHandler(), null));
		this.delegate = delegate;
		this.capture = capture;
	}

	@Override
	public void initializeMetadata() {
		delegate.initializeMetadata();
	}

	@Override
	public boolean initializeStateStores() {
		return delegate.initializeStateStores();
	}

	@Override
	public void initializeTopology() {
		delegate.initializeTopology();
	}

	@Override
	public void resume() {
		delegate.resume();
	}

	@Override
	public boolean isProcessable(long now) {
		return delegate.isProcessable(now);
	}

	@Override
	public boolean process() {
		return delegate.process();
	}

	@Override
	public void punctuate(ProcessorNode node, long timestamp, PunctuationType type, Punctuator punctuator) {
		delegate.punctuate(node, timestamp, type, punctuator);
	}

	@Override
	public void commit() {
		delegate.commit();
	}

	@Override
	public void flushState() {
		delegate.flushState();
	}

	@Override
	public void suspend() {
		delegate.suspend();
	}

	@Override
	public void close(boolean clean, boolean isZombie) {
		delegate.close(clean, isZombie);
	}

	@Override
	public void addRecords(TopicPartition partition, Iterable<ConsumerRecord<byte[], byte[]>> records) {
		for (ConsumerRecord<byte[], byte[]> record : records) {
			capture.invoke(delegate, partition, record);
		}
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
	public boolean hasRecordsQueued() {
		return delegate.hasRecordsQueued();
	}

	@Override
	public TaskId id() {
		return delegate.id();
	}

	@Override
	public String applicationId() {
		return delegate.applicationId();
	}

	@Override
	public Set<TopicPartition> partitions() {
		return delegate.partitions();
	}

	@Override
	public ProcessorTopology topology() {
		return delegate.topology();
	}

	@Override
	public ProcessorContext context() {
		return delegate.context();
	}

	@Override
	public StateStore getStore(String name) {
		return delegate.getStore(name);
	}

	@Override
	public String toString() {
		return delegate.toString();
	}

	@Override
	public boolean isEosEnabled() {
		return delegate.isEosEnabled();
	}

	@Override
	public String toString(String indent) {
		return delegate.toString(indent);
	}

	@Override
	public boolean isClosed() {
		return delegate.isClosed();
	}

	@Override
	public boolean commitNeeded() {
		return delegate.commitNeeded();
	}

	@Override
	public boolean hasStateStores() {
		return delegate.hasStateStores();
	}

	@Override
	public Collection<TopicPartition> changelogPartitions() {
		return delegate.changelogPartitions();
	}
}
