package com.app.notification.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TopicConfig {

	@Bean
	public NewTopic getNewTopic()
	{
		String name = "SupplierConsumer";
		int numPartitions = 1;
		short replicationFactor = 1;
		
		return new NewTopic(name, numPartitions, replicationFactor);
	}
}
