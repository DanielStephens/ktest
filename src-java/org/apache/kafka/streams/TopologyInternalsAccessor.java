package org.apache.kafka.streams;

import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.ProcessorTopologyAccessor;
import org.apache.kafka.streams.processor.internals.StreamTask;

public class TopologyInternalsAccessor {

	private TopologyInternalsAccessor() {
	}

	public static InternalTopologyBuilder internalTopologyBuilder(Topology topology) {
		return topology.internalTopologyBuilder;
	}

	public static ProcessorTopology processorTopology(TopologyTestDriver topologyTestDriver) {
		return topologyTestDriver.processorTopology;
	}

	public static ProcessorTopology globalProcessorTopology(TopologyTestDriver topologyTestDriver) {
		return topologyTestDriver.globalTopology;
	}

	public static StreamTask getTestStreamTask(TopologyTestDriver topologyTestDriver) {
		return topologyTestDriver.task;
	}

	public static boolean isRepartitionTopic(ProcessorTopology processorTopology, String topic) {
		return ProcessorTopologyAccessor.isRepartitionTopic(processorTopology, topic);
	}

}
