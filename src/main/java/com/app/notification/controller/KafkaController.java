package com.app.notification.controller;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.app.notification.kafka.KafkaProducer;
import com.app.notification.kafka.KafkaTopicsModel;
import com.app.notification.kafka.PublishMessageModel;
import com.app.notification.kafka.SubscribeTopicModel;
import com.app.notification.kafka.kafkaTopicsWrapper;
import com.app.notification.kafka.service.KafkaService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Api(tags = "Notification Management RESTful Services", value = "Notification services", description = "Controller for Notification Management Service")
@RestController
@RequestMapping("/api")
public class KafkaController {

	@Autowired
	KafkaService kafkaService;

	@Autowired
	KafkaAdmin kafkaAdmin;

	@Autowired
	KafkaProducer kafkaProducer;

	@ApiOperation(value = "Created kafka Topic", response = kafkaTopicsWrapper.class)
	@ApiResponses(value = { @ApiResponse(code = 201, message = "Successfully Created Topic"),
			@ApiResponse(code = 401, message = "You are not authorized to view the resource"),
			@ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
			@ApiResponse(code = 404, message = "The resource you were trying to reach is not found"),

	})
	@PostMapping(value = "/create", consumes = "application/json", produces = "application/json")
	private void createTopic(@RequestBody kafkaTopicsWrapper topicNames) {
		try {
			Collection<NewTopic> topicsCollection = new ArrayList<>();
			for (KafkaTopicsModel topicModel : topicNames.getTopics()) {
				topicsCollection.add(new NewTopic(topicModel.getTopicName(), 1, (short) 1));
			}
			kafkaService.addTopicsIfNeeded(AdminClient.create(kafkaAdmin.getConfig()), topicsCollection);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@ApiOperation(value = "Publish the message to kafka Topic", response = PublishMessageModel.class)
	@ApiResponses(value = { @ApiResponse(code = 201, message = "Successfully Publish the Message from Topic"),
			@ApiResponse(code = 401, message = "You are not authorized to view the resource"),
			@ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
			@ApiResponse(code = 404, message = "The resource you were trying to reach is not found"),

	})
	@PostMapping(value = "/publish", consumes = "application/json", produces = "application/json")
	private ResponseEntity<String> publishMessage(@RequestBody PublishMessageModel topicObj) {
		try {
			kafkaProducer.sendMessage(topicObj.getTopicName(), topicObj.getMessage());
			return new ResponseEntity<String>(HttpStatus.CREATED);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	@ApiOperation(value = "Subscribe the message from kafka Topic", response = SubscribeTopicModel.class)
	@ApiResponses(value = { @ApiResponse(code = 201, message = "Successfully Subscribe the message from Topic"),
			@ApiResponse(code = 401, message = "You are not authorized to view the resource"),
			@ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
			@ApiResponse(code = 404, message = "The resource you were trying to reach is not found"),

	})
	@PostMapping(value = "/subscribe", consumes = "application/json", produces = "application/json")
	private void subscribeTopic(@RequestBody SubscribeTopicModel topicObj) {
		try {
			kafkaService.subscribe(AdminClient.create(kafkaAdmin.getConfig()), topicObj);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
