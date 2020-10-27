package com.practice.model;

public class FileOperationResponse {
    private final String remoteFilePath;
    private final long uploadSize;

    public FileOperationResponse() {
        remoteFilePath = "";
        uploadSize = 0;
    }

    public FileOperationResponse(final String remoteFilePath, final long uploadSize) {
        this.remoteFilePath = remoteFilePath;
        this.uploadSize = uploadSize;
    }

    public String getRemoteFilePath() {
        return remoteFilePath;
    }

    public long getUploadSize() {
        return uploadSize;
    }
}
