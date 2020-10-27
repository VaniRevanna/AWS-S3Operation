package com.practice.constants;

public class StoreFilePayload {
    private String fileName;
    private String fileContent;
    private String server;
    private String port;
    private String remoteDir;

    public StoreFilePayload() {

    }

    public StoreFilePayload(String fileName, String fileContent, String server, String port, String remoteDir) {
        this.fileName = fileName;
        this.fileContent = fileContent;
        this.server = server;
        this.port = port;
        this.remoteDir = remoteDir;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileContent() {
        return fileContent;
    }

    public void setFileContent(String fileContent) {
        this.fileContent = fileContent;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getRemoteDir() {
        return remoteDir;
    }

    public void setRemoteDir(String remoteDir) {
        this.remoteDir = remoteDir;
    }
}
