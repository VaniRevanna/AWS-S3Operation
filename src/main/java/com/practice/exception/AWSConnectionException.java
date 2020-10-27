package com.practice.exception;

import com.practice.constants.FileTransferErrors.*;
import com.practice.constants.ConfigurationConsts.*;
import com.practice.constants.FileTransferErrors;

public class AWSConnectionException extends AWSException {
    private int statusCode;

   

    public AWSConnectionException(final int statusCode, final String message) {
        super(message, FileTransferErrors.NETWORK_ERROR);
        this.statusCode = statusCode;
    }

    public AWSConnectionException(final String message) {
        super(message, FileTransferErrors.NETWORK_ERROR);
    }

    public AWSConnectionException(final String message,
                                      final Throwable cause) {
        super(message, cause,  FileTransferErrors.NETWORK_ERROR);
    }

    public AWSConnectionException(final int statusCode,
                                      final String message,
                                      final Throwable cause) {
        super(message, cause,  FileTransferErrors.NETWORK_ERROR);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }
}
