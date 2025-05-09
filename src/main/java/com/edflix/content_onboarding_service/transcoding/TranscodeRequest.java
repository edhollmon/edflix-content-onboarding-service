package com.edflix.content_onboarding_service.transcoding;

public class TranscodeRequest {
    private String url;
    private String contentProviderId;
    private String contentId;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getContentProviderId() {
        return contentProviderId;
    }

    public void setContentProviderId(String contentProviderId) {
        this.contentProviderId = contentProviderId;
    }

    public String getContentId() {
        return contentId;
    }

    public void setContentId(String contentId) {
        this.contentId = contentId;
    }

    @Override
    public String toString() {
        return "TranscodeRequest{" +
                "url='" + url + '\'' +
                ", contentProviderId='" + contentProviderId + '\'' +
                ", contentId='" + contentId + '\'' +
                '}';
    }
}