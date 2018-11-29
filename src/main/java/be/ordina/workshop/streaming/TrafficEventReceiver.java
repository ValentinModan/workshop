package be.ordina.workshop.streaming;

import be.ordina.workshop.streaming.domain.TrafficEvent;
import be.ordina.workshop.streaming.domain.VehicleClass;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

/**
 * @author Tim Ysewyn
 */
@Component
// Spring Cloud Stream with Kafka
//@EnableBinding(Sink.class)

//	Native stream processing with Kafka Streams
@EnableBinding(TrafficEventReceiver.KStreamSink.class)
public class TrafficEventReceiver {

	public static final Logger logger = LoggerFactory.getLogger(TrafficEventReceiver.class);

//	Spring Cloud Stream with Kafka

//	@StreamListener(Sink.INPUT)
//	public void receiveData(TrafficEvent trafficEvent) {
//		logger.info("We received: {}", trafficEvent);
//	}

//	Native stream processing with Kafka Streams
	@StreamListener
	public void filterAndLogCars(@Input("native-input") KStream<Object, TrafficEvent> kstream) {
		kstream.filter(((key, trafficEvent) -> VehicleClass.CAR == trafficEvent.getVehicleClass()))
				.selectKey((key, value) -> value.getSensorId())
				.groupByKey(Serialized.with(Serdes.String(), new JsonSerde<>(TrafficEvent.class)))
				.windowedBy(TimeWindows.of(120_000L))
				.aggregate(Average::new, (sensorId, trafficEvent, average) -> {
					average.addSpeed(trafficEvent.getTrafficIntensity(),
							trafficEvent.getVehicleSpeedCalculated());
					return average;
				}, Materialized.with(Serdes.String(), new JsonSerde<>(Average.class)))
				.mapValues(Average::average)
				.toStream()
				.print(Printed.toSysOut());
	}

	private static class Average {

		private int amountOfCars = 0;
		private int totalSpeed = 0;

		private Average() {
		}

		@JsonCreator
		public Average(@JsonProperty("amountOfCars") int amountOfCars,
					   @JsonProperty("totalSpeed") int totalSpeed) {
			this.amountOfCars = amountOfCars;
			this.totalSpeed = totalSpeed;
		}

		public int getAmountOfCars() {
			return this.amountOfCars;
		}

		public int getTotalSpeed() {
			return this.totalSpeed;
		}

		private void addSpeed(int amountOfCars, int speed) {
			this.amountOfCars += amountOfCars;
			this.totalSpeed += speed;
		}

		private double average() {
			if (this.amountOfCars == 0) {
				return 0;
			}
			return this.totalSpeed / this.amountOfCars;
		}
	}

	interface KStreamSink {

		@Input("native-input")
		KStream<?, ?> input();

	}

}
