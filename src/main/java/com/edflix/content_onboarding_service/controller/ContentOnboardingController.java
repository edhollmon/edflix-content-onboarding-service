package com.edflix.content_onboarding_service.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
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

    @Value("${aws.sqs.content-onboarding-queue-url}")
    private String contentOnboardingQueueUrl;

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
            item.put("url", AttributeValue.builder().s(url).build());
            item.put("contentProviderId", AttributeValue.builder().s(contentProviderId).build());

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
            // Publish to SQS
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(contentOnboardingQueueUrl)
                    .messageBody(String.format("Content onboarded: {contentId: %s, url: %s, contentProviderId: %s}", uniqueId, url, contentProviderId))
                    .build();

            sqsClient.sendMessage(sendMessageRequest);
            logger.info("Successfully published message to SQS for content ID: {}", uniqueId);

        } catch (Exception e) {
            logger.error("Failed to publish message to SQS", e);
            return ResponseEntity.status(500).body("Failed to onboard content: SQS error");
        }

        return ResponseEntity.ok("Content onboarded successfully with ID: " + uniqueId);
    }
}
