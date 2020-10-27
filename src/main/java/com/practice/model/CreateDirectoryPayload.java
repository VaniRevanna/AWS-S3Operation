package com.practice.model;

public class CreateDirectoryPayload {
    private String protocol = "sftp";
    private String rootDirectory;
    private String createPath;
    private boolean createRecursive = true;

    public CreateDirectoryPayload() {

    }

    public CreateDirectoryPayload(String rootDirectory,
                                  String createPath) {
        this.rootDirectory = rootDirectory;
        this.createPath = createPath;
    }

    public CreateDirectoryPayload(String rootDirectory,
                                  String createPath,
                                  boolean createRecursive) {
        this.createRecursive = createRecursive;
        this.rootDirectory = rootDirectory;
        this.createPath = createPath;
    }


    public String getProtocol() {
        return protocol;
    }

    public String getRootDirectory() {
        return rootDirectory;
    }

    public String getCreatePath() {
        return createPath;
    }

    public boolean isCreateRecursive() {
        return createRecursive;
    }
}
