package com.practice.exception;

import com.practice.constants.FileTransferErrors;

public class AlreadyExistsException extends FileTransferException {


    public AlreadyExistsException(final String message) {
        super(message, FileTransferErrors.ALREADY_EXISTS.getCode());
    }

    public AlreadyExistsException(final String message,
                                      final Throwable cause) {
        super(message, cause,  FileTransferErrors.ALREADY_EXISTS.getCode());
    }
}
