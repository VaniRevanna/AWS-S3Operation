package com.practice.model;



import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import javax.tools.FileObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.practice.s3.service.S3OperationsManager;

public class FileObjectProxy implements Closeable {
   
    private InputStream fileStream;
    private FileObject fileObject;
    private boolean isStreaming;

    public FileObjectProxy(final InputStream fileStream,
                           final FileObject fileObject) {
        this.fileStream = fileStream;
        this.fileObject = fileObject;
        this.isStreaming = false;
    }

    public FileObjectProxy(final InputStream fileStream,
                           final FileObject fileObject,
                           final boolean isStreaming) {
        this.fileStream = fileStream;
        this.fileObject = fileObject;
        this.isStreaming = isStreaming;
    }

    public InputStream getFileStream() {
        return fileStream;
    }

    public void setFileStream(InputStream fileStream) {
        this.fileStream = fileStream;
    }

    public FileObject getFileObject() {
        return fileObject;
    }

    public void setFileObject(FileObject fileObject) {
        this.fileObject = fileObject;
    }

    @Override
    public void close() {
        try {
            if(null != fileStream) {
                fileStream.close();
            }
           
        } catch (IOException e) {
            this.getlogger().error(String.format("%s", e));
        }
    }

   
    public boolean isStreaming() {
        return isStreaming;
    }

    public void setStreaming(boolean streaming) {
        isStreaming = streaming;
    }
    
    /**
  	 * Initializing the logger
  	 * 
  	 * @return
  	 */
  	private Logger getlogger() {
  		return LoggerFactory.getLogger(FileObjectProxy.class);
  	}
}
