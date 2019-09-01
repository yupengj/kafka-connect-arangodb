package com.gant.kafka.connect.arangodb.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class KafkaTopicUtils {

	private static AdminClient adminClient;

	@BeforeAll
	public static void before() {
		adminClient = AdminClient.create(Collections.singletonMap(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.4.109:9092"));
	}

	@AfterAll
	public static void after() {
		if (adminClient != null) {
			adminClient.close();
		}
	}

	@Test
	public void testFindAllTopics() throws ExecutionException, InterruptedException {
		Set<String> allTopics = findAllTopics();
		System.out.println("topic size " + allTopics.size());
		allTopics.forEach(System.out::println);
	}


	private Set<String> findAllTopics() throws ExecutionException, InterruptedException {
		ListTopicsResult listTopicsResult = adminClient.listTopics();
		Collection<TopicListing> topicListings = listTopicsResult.listings().get();
		Set<String> topicNames = topicListings.stream().filter(it -> !it.isInternal()).map(TopicListing::name).collect(Collectors.toSet());
		return topicNames;
	}

	//	@Test
	public void findDescAllTopics() throws ExecutionException, InterruptedException {
		Set<String> allTopics = findAllTopics();
		allTopics.removeIf(it -> !it.equals("dgraph.created.uid"));
		DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(allTopics);
		Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
		stringTopicDescriptionMap.forEach((k, v) -> {
			System.out.println("tpoic: " + v + " desc: " + v);
		});
	}

	//	@Test
	public void testDelete() throws ExecutionException, InterruptedException {
//		String[] topics = {"ibom.bommgmt.bm_part_assemblyffffffda"};
//		List<String> allTopics = Arrays.asList(topics);
//
		Set<String> allTopics = findAllTopics();
		adminClient.deleteTopics(allTopics);
	}


//	@Test
//	public void testConsumerForeignKeyConstraintTopic() {
//		Properties props = new Properties();
//		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, dgraphSinkConfig.uidTopicBootstrapServers);
//		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test1_1");
//		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
////		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
//		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
//
//		KafkaConsumer<JsonNode, JsonNode> kafkaConsumer = new KafkaConsumer<>(props);
//		kafkaConsumer.subscribe(Collections.singletonList("ibom.mstdata.md_foreign_key_constraint"));
//
//		int num = 15;
//		int count = 0;
//		while (true) {
//			if (num < 0) {
//				break;
//			}
//			ConsumerRecords<JsonNode, JsonNode> records = kafkaConsumer.poll(Duration.ofSeconds(1));
//			int tempCount = count + records.count();
//			System.out.println("records size: " + tempCount);
//			for (ConsumerRecord<JsonNode, JsonNode> record : records) {
//				System.out.println(
//						"Received message: (" + record.key() + ", " + record.value() + ") at partition: " + record.partition() + ", offset " + record.offset());
//			}
//			if (tempCount == count) {
//				num--;
//			}
//			count = tempCount;
//		}
//		System.out.println("count: " + count);
//	}
//
//	@Test
//	public void testConsumerIBomTopic() {
//		Properties props = new Properties();
//		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, dgraphSinkConfig.uidTopicBootstrapServers);
//		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test1_1");
//		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
//		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
//
//		KafkaConsumer<JsonNode, JsonNode> kafkaConsumer = new KafkaConsumer<>(props);
//		kafkaConsumer.subscribe(Collections.singletonList("ibom.bommgmt.bm_part_assembly"));
//
//		long start = System.currentTimeMillis();
//
//		int num = 15;
//		int count = 0;
//		while (true) {
//			if (num < 0) {
//				break;
//			}
//			ConsumerRecords<JsonNode, JsonNode> records = kafkaConsumer.poll(Duration.ofSeconds(1));
//			int tempCount = count + records.count();
//			System.out.println("records size: " + tempCount);
//			for (ConsumerRecord<JsonNode, JsonNode> record : records) {
//				System.out.println(
//						"Received message: (" + record.key() + ", " + record.value() + ") at partition: " + record.partition() + ", offset " + record.offset());
//			}
//			if (tempCount == count) {
//				num--;
//			}
//			count = tempCount;
//		}
//		System.out.println("消费 bom 主题，size " + count + " time " + (System.currentTimeMillis() - start) + " ms");
//	}
}
