package com.example.demokafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static java.util.Objects.nonNull;

@SpringBootApplication
public class DemoKafkaApplication {

	private static final String msg = "Minha mensagem";
	private static final String topic = "Minha mensagem";

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		SpringApplication.run(DemoKafkaApplication.class, args);

		var producer = new KafkaProducer<String, String>(obterProperties());
		var record = new ProducerRecord<String,String>(topic,msg,msg);

		sendMsg(producer, record);
	}

	private static void sendMsg(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) throws InterruptedException, ExecutionException {
		producer.send(record,(dados, exception)->{
			if(nonNull(exception)){
				exception.printStackTrace();
				return;
			}
			System.out.println(dados.topic() +" | offset: "+dados.offset());
		}).get();
	}

	private static Properties obterProperties() {
		var properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;
	}

}
