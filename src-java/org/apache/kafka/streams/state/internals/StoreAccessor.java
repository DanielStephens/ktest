package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class StoreAccessor {

	private StoreAccessor(){}

	public static <K> Serde<K> keySerde(AbstractStoreBuilder<K, ?, ?> builder){
		return builder.keySerde;
	}

	public static <V> Serde<V> valueSerde(AbstractStoreBuilder<?, V, ?> builder){
		return builder.valueSerde;
	}

	public static <V> Serde<V> deTimestampSerde(ValueAndTimestampSerde<V> serde){
		return Serdes.serdeFrom(
				((ValueAndTimestampSerializer<V>)serde.serializer()).valueSerializer,
				((ValueAndTimestampDeserializer<V>)serde.deserializer()).valueDeserializer
		);
	}

}
