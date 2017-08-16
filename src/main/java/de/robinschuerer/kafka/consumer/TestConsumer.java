package de.robinschuerer.kafka.consumer;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class TestConsumer {

	private static final Logger LOGGER = LoggerFactory.getLogger(TestConsumer.class);

	private static final AtomicLong COUNT = new AtomicLong();

	@Value("${consumerSleepInterval}")
	private long sleep;

	@KafkaListener(topics = "${kafka.topic}")
	public void handle(String payload) {
		LOGGER.info("Received Message: {}, count: {}", payload, COUNT.incrementAndGet());

		try {
			Thread.sleep(sleep);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}

	}



}
