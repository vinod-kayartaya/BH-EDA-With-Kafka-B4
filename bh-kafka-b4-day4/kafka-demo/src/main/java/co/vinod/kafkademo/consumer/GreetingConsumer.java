package co.vinod.kafkademo.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class GreetingConsumer {
    
    @KafkaListener(topics = "greet", groupId = "greeting-group")
    public void consume(String message) {
        System.out.println("Received message: " + message);
    }
}
