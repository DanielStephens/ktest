package org.apache.kafka.streams.processor.internals;

public class ProcessorTopologyAccessor {

	private ProcessorTopologyAccessor(){}

	public static boolean isRepartitionTopic(ProcessorTopology processorTopology, String topic){
		return processorTopology.isRepartitionTopic(topic);
	}

}
