package com.practice.model;

public class DeleteFolderPayLoad {
    private String server = "";
    private int port = 0;
    private String userName = "";
    private String password = "";
    private String directorName;

    private String regularExpressionString = "";

    public DeleteFolderPayLoad() {
    }

    public DeleteFolderPayLoad(final String server,
                        final int port,
                        final String directorName,
                        final String userName,
                        final String password) {
        this.server = server;
        this.port = port;
        this.directorName = directorName;
        this.userName = userName;
        this.password = password;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDirectorName() {
        return directorName;
    }

    public void setDirectorName(String directorName) {
        this.directorName = directorName;
    }

    public String getRegularExpressionString() {
        return regularExpressionString;
    }

    public void setRegularExpressionString(String regularExpressionString) {
        this.regularExpressionString = regularExpressionString;
    }
}

