package org.apache.kafka.streams;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Properties;

import org.apache.kafka.streams.internals.QuietStreamsConfig;
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

	public static void setTestStreamTask(TopologyTestDriver topologyTestDriver, StreamTask streamTask) {
		try {
			Field modifiersField = Field.class.getDeclaredField("modifiers");
			modifiersField.setAccessible(true);
			Field field = TopologyTestDriver.class.getDeclaredField("task");
			modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
			field.set(topologyTestDriver, streamTask);
		} catch (IllegalAccessException | NoSuchFieldException e) {
			throw new RuntimeException(e);
		}
	}

	public static boolean isRepartitionTopic(ProcessorTopology processorTopology, String topic){
		return ProcessorTopologyAccessor.isRepartitionTopic(processorTopology, topic);
	}

}
