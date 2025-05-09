package com.edflix.content_onboarding_service.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/content")
public class ContentOnboardingController {

    private static final Logger logger = LoggerFactory.getLogger(ContentOnboardingController.class);

    @Value("${aws.sqs.content-transcoding-queue-url}")
    private String contentTranscodingQueueUrl;

    private final DynamoDbClient dynamoDbClient;
    private final SqsClient sqsClient;

    public ContentOnboardingController(DynamoDbClient dynamoDbClient, SqsClient sqsClient) {
        this.dynamoDbClient = dynamoDbClient;
        this.sqsClient = sqsClient;
    }

    @PostMapping("/onboard")
    public ResponseEntity<String> onboardContent(@RequestParam String url, @RequestParam String contentProviderId) {
        String uniqueId = UUID.randomUUID().toString();

        try {
            // Store in DynamoDB
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("contentId", AttributeValue.builder().s(uniqueId).build());
            item.put("contentProviderId", AttributeValue.builder().s(contentProviderId).build());
            item.put("url", AttributeValue.builder().s(url).build());
            item.put("status", AttributeValue.builder().s("TRANSCODING").build());
            item.put("res_urls", AttributeValue.builder().m(new HashMap<>()).build()); // Initialize res_urls as an empty map

            PutItemRequest putItemRequest = PutItemRequest.builder()
                    .tableName("ContentOnboarding")
                    .item(item)
                    .build();

            dynamoDbClient.putItem(putItemRequest);
            logger.info("Successfully stored content in DynamoDB with ID: {}", uniqueId);

        } catch (Exception e) {
            logger.error("Failed to store content in DynamoDB", e);
            return ResponseEntity.status(500).body("Failed to onboard content: DynamoDB error");
        }

        try {
            // Create JSON message body
            Map<String, String> messageBody = new HashMap<>();
            messageBody.put("contentId", uniqueId);
            messageBody.put("url", url);
            messageBody.put("contentProviderId", contentProviderId);

            String jsonMessageBody = new ObjectMapper().writeValueAsString(messageBody);

            // Publish to SQS
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(contentTranscodingQueueUrl)
                    .messageBody(jsonMessageBody)
                    .build();

            sqsClient.sendMessage(sendMessageRequest);
            logger.info("Successfully published message to SQS for content ID: {}", uniqueId);

        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize message body to JSON", e);
            return ResponseEntity.status(500).body("Failed to onboard content: JSON serialization error");
        } catch (Exception e) {
            logger.error("Failed to publish message to SQS", e);
            return ResponseEntity.status(500).body("Failed to onboard content: SQS error");
        }

        return ResponseEntity.ok("Content onboarded successfully with ID: " + uniqueId);
    }

    @PostMapping("/onboard/transcode")
    public ResponseEntity<String> addTranscode(@RequestParam String contentId, 
                                                 @RequestParam String resolutionKey, 
                                                 @RequestParam String resolution, 
                                                 @RequestParam String url) {
        try {
            // Store resolution in DynamoDB
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("contentId", AttributeValue.builder().s(contentId).build()); // PK
            item.put("resolutionKey", AttributeValue.builder().s("RES#" + resolutionKey).build()); // SK
            item.put("resolution", AttributeValue.builder().s(resolution).build());
            item.put("url", AttributeValue.builder().s(url).build());

            PutItemRequest putItemRequest = PutItemRequest.builder()
                    .tableName("ContentOnboarding") // Store in different table if needed
                    .item(item)
                    .build();

            dynamoDbClient.putItem(putItemRequest);
            logger.info("Successfully added resolution for content ID: {}, resolutionKey: {}", contentId, resolutionKey);

        } catch (Exception e) {
            logger.error("Failed to add resolution to DynamoDB", e);
            return ResponseEntity.status(500).body("Failed to add resolution: DynamoDB error");
        }

        return ResponseEntity.ok("Resolution added successfully for content ID: " + contentId + ", resolutionKey: " + resolutionKey);
    }
}
