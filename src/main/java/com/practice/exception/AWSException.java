package com.practice.exception;

import com.practice.constants.FileTransferErrors;

public class AWSException extends FileTransferException {


    public AWSException(final String message) {
        super(message,FileTransferErrors.INVALID_INPUT.getCode());
    }

    public AWSException(final String message,FileTransferErrors code) {
        super(message,code.getCode());
    }
    public AWSException(final String message,
                            final Throwable cause) {
        super(message, cause, FileTransferErrors.INVALID_INPUT.getCode());
    }

    public AWSException(final String message,
            final Throwable cause,FileTransferErrors code) {
super(message, cause, code.getCode());
}
}
