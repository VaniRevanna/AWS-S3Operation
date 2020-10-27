package com.practice.exception;

import com.practice.constants.FileTransferErrors;

public class RemoteFileNotFoundException extends FileTransferException {

   
    public RemoteFileNotFoundException(final String message) {
        super(message, FileTransferErrors.FILE_NOT_FOUND.getCode());
    }

    public RemoteFileNotFoundException(final String message,
                                           final Throwable cause) {
        super(message, cause, FileTransferErrors.FILE_NOT_FOUND.getCode());
    }
}
