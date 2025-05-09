package com.edflix.content_onboarding_service.transcoding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.awspring.cloud.sqs.annotation.SqsListener;

import org.springframework.stereotype.Service;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class CompletedTranscodedContentConsumer {

    private final ObjectMapper objectMapper;
    private final DynamoDbClient dynamoDbClient;

    public CompletedTranscodedContentConsumer(ObjectMapper objectMapper, DynamoDbClient dynamoDbClient) {
        this.objectMapper = objectMapper;
        this.dynamoDbClient = dynamoDbClient;
    }

    @SqsListener("${aws.sqs.completed-transcoded-content-queue-url}")
    public void listenToCompletedTranscodedContentQueue(String message) {
        try {
            JsonNode rootNode = objectMapper.readTree(message);
            TranscodeRequest transcodeRequest = extractTranscodeRequest(rootNode);
            Map<String, AttributeValue> resolutionsMap = extractResolutionsMap(rootNode);

            updateDynamoDb(transcodeRequest.getContentId(), resolutionsMap);
        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private TranscodeRequest extractTranscodeRequest(JsonNode rootNode) throws Exception {
        JsonNode userMetadataNode = rootNode.path("detail").path("userMetadata");
        TranscodeRequest transcodeRequest = objectMapper.treeToValue(userMetadataNode, TranscodeRequest.class);
        System.out.println("Extracted TranscodeRequest: " + transcodeRequest);
        return transcodeRequest;
    }

    private Map<String, AttributeValue> extractResolutionsMap(JsonNode rootNode) {
        Map<String, AttributeValue> resolutionsMap = new HashMap<>();
        JsonNode outputGroupDetails = rootNode.path("detail").path("outputGroupDetails");

        for (JsonNode outputGroup : outputGroupDetails) {
            JsonNode outputDetails = outputGroup.path("outputDetails");
            for (JsonNode outputDetail : outputDetails) {
                List<String> outputFilePaths = objectMapper.convertValue(
                        outputDetail.path("outputFilePaths"), new com.fasterxml.jackson.core.type.TypeReference<List<String>>() {});

                for (String filePath : outputFilePaths) {
                    String httpsFilePath = filePath.replace("s3://", "https://");
                    String resolution = outputDetail.path("videoDetails").path("height").asText() + "p";
                    resolutionsMap.put(resolution, AttributeValue.builder().s(httpsFilePath).build());
                }
            }
        }
        return resolutionsMap;
    }

    private void updateDynamoDb(String contentId, Map<String, AttributeValue> resolutionsMap) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("contentId", AttributeValue.builder().s(contentId).build());

        String updateExpression = "SET res_urls = :resolutionsMap, #status = :status";
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#status", "status");

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":resolutionsMap", AttributeValue.builder().m(resolutionsMap).build());
        expressionAttributeValues.put(":status", AttributeValue.builder().s("TRANSCODINGCOMPLETE").build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName("ContentOnboarding")
                .key(key)
                .updateExpression(updateExpression)
                .expressionAttributeNames(expressionAttributeNames)
                .expressionAttributeValues(expressionAttributeValues)
                .build();

        dynamoDbClient.updateItem(updateRequest);
        System.out.println("Updated DynamoDB item with resolutions and status: TRANSCODINGCOMPLETE");
    }
}
