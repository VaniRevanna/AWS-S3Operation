package com.practice.exception;

import com.practice.constants.FileTransferErrors;

public class RemoteFolderNameWrongException extends FileTransferException {

   

    public RemoteFolderNameWrongException(final String message) {

        super(message, FileTransferErrors.INVALID_REMOTE_LOCATION.getCode());
    }

    public RemoteFolderNameWrongException(final String message,
                                              final Throwable cause) {
        super(message, cause, FileTransferErrors.INVALID_REMOTE_LOCATION.getCode());
    }
}
