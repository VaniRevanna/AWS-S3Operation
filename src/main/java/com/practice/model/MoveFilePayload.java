package com.practice.model;


public class MoveFilePayload {
    public String fromServerName = "";
    public int fromServerPort = 222;
    public String fromUserName = "";
    public String fromPassword = "";
    public String fromLocation;

    public String toServerName = "";
    public int toServerPort = 222;
    public String toUserName = "";
    public String toPassword = "";
    public String toRootDirectory;
    public String toLocationName;

    public boolean overwrite = true;

    public boolean createToRootIfNotExists = true;

    public void setFromServerName(String fromServerName) {
        this.fromServerName = fromServerName;
    }

    public void setFromServerPort(int fromServerPort) {
        this.fromServerPort = fromServerPort;
    }

    public void setFromUserName(String fromUserName) {
        this.fromUserName = fromUserName;
    }

    public void setFromPassword(String fromPassword) {
        this.fromPassword = fromPassword;
    }

    public void setFromLocation(String fromLocation) {
        this.fromLocation = fromLocation;
    }

    public void setToServerPort(int toServerPort) {
        this.toServerPort = toServerPort;
    }

    public void setToUserName(String toUserName) {
        this.toUserName = toUserName;
    }

    public void setToPassword(String toPassword) {
        this.toPassword = toPassword;
    }

    public void setToRootDirectory(String toRootDirectory) {
        this.toRootDirectory = toRootDirectory;
    }

    public void setToLocationName(String toLocationName) {
        this.toLocationName = toLocationName;
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public void setCreateToRootIfNotExists(boolean createToRootIfNotExists) {
        this.createToRootIfNotExists = createToRootIfNotExists;
    }

    public String getFromServerName() {
        return fromServerName;
    }

    public int getFromServerPort() {
        return fromServerPort;
    }

    public String getFromUserName() {
        return fromUserName;
    }

    public String getFromPassword() {
        return fromPassword;
    }

    public String getFromLocation() {
        return fromLocation;
    }

    public String getToServerName() {
        return toServerName;
    }

    public int getToServerPort() {
        return toServerPort;
    }

    public String getToUserName() {
        return toUserName;
    }

    public String getToPassword() {
        return toPassword;
    }

    public String getToRootDirectory() {
        return toRootDirectory;
    }

    public String getToLocationName() {
        return toLocationName;
    }

    public boolean getOverwrite() { return overwrite; }

    public boolean getCreateToRootIfNotExists() {
        return createToRootIfNotExists;
    }
}
