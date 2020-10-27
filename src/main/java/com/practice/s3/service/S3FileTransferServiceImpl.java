package com.practice.s3.service;

import com.newrelic.api.agent.Trace;
import com.practice.constants.ErrorConstants;
import com.practice.constants.FileSystemOptionKeys;
import com.practice.exception.CannotFetchRemoteFileException;
import com.practice.exception.FileTransferException;
import com.practice.exception.InvalidFileNamePatternException;
import com.practice.exception.RemoteFolderNameWrongException;
import com.practice.model.CreateDirectoryPayload;
import com.practice.model.DeleteFolderPayLoad;
import com.practice.model.FileObjectProxy;
import com.practice.model.FileOperationResponse;
import com.practice.model.MoveFilePayload;
import com.practice.model.Pair;

import ch.qos.logback.classic.Logger;

import org.apache.commons.io.IOUtils;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * {@link S3FileTransferServiceImpl} is the an implementation of the {@link FileTransferService} class.
 * This class manages the transfer of file to and from S3 location. Internally this uses the {@link S3OperationsManager}
 * class.
 */
@Component("S3FileTransferServiceImpl")
@RefreshScope
public class S3FileTransferServiceImpl implements FileTransferService {
    private static Logger logger = (Logger) LoggerFactory.getLogger(S3FileTransferServiceImpl.class);

    /**
     * The S3 utility class that handles all the S3 related operations.
     */
    private final S3OperationsManager s3OperationsManager;

    /**
     * This is the temporary directory where all the file related operations are handled.
     */
    @Value("${file.operation-dir}")
    private String fileOperationDirectory;

    /**
     * Constructor for the class.
     */
    @Autowired
    public S3FileTransferServiceImpl(S3OperationsManager s3OperationsManager) {
        logger.info("fileOperationDirectory: " + fileOperationDirectory);
        this.s3OperationsManager = s3OperationsManager;
    }

    /**
     * This method is used to upload a file to the given location. This method uses the multipart upload mechanism.
     * @param multipartFile The file that has to be uploaded.
     * @param optionsObjectMap The options that are available.
     * @param entityId The entity id if available. This is used for logging.
     * @param connectorName The connector name if available. This is used for logging
     * @return {@link FileOperationResponse} The response of the operation done on the file.
     * @throws GISTFileTransferExceptions The exception is thrown
     */
    @Trace
    @Override
    public Optional<FileOperationResponse> uploadFile(final MultipartFile multipartFile,
                                             final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
                                             String entityId, String connectorName) throws FileTransferException {
        final String userDirectory = (String) optionsObjectMap.get(FileSystemOptionKeys.REMOTE_DIR);
        if(userDirectory.isEmpty()) {
            throw new RemoteFolderNameWrongException(ErrorConstants.REMOTE_FOLDER_NOT_FOUND);
        }
        return s3OperationsManager.uploadFile(multipartFile, optionsObjectMap, entityId, connectorName);
    }

    /**
     * This method is used to download file from the remote S3 location as a multipart file.
     * @param fullFileName File name to download
     * @param optionsObjectMap Options needed for the file download
     * @param entityId The entity id if available. This is used for logging.
     * @param connectorName The connector name if available. This is used for logging
     * @return {@link FileOperationResponse} The response of the operation done on the file and the path of the file.
     * @throws GISTFileTransferExceptions The exception is thrown
     */
    @Trace
    @Override
    public Optional<Pair<FileOperationResponse, String>> downloadFile(final String fullFileName,
                                                 final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
                                                 String entityId, String connectorName) throws FileTransferException {
        final Optional<Path> pathOptional = s3OperationsManager.downloadFile(fullFileName, fileOperationDirectory, optionsObjectMap, entityId, connectorName);
        if(!pathOptional.isPresent()) {
            throw new FileTransferException(
                    ErrorConstants.CANNOT_FETCH_FILE + ErrorConstants.DISPLAY_SPACE + fullFileName);
        }
        final Path path = pathOptional.get();
        final File file = path.toFile();
        final FileOperationResponse fileOperationResponse =
                new FileOperationResponse(file.getPath(), file.length());
        final Pair<FileOperationResponse, String> responseStringPair = new Pair<>(fileOperationResponse, file.getPath());
        return Optional.of(responseStringPair);
    }

    /**
     * This method is used to list the remote files
     * @param optionsObjectMap The list of options that is needed for this method to work.
     * @param entityId The entity id if available. This is used for logging.
     * @param connectorName The connector name if available. This is used for logging
     * @return {@link List} of {@link String} if the method is success else no value will be present.
     * @throws GISTFileTransferExceptions The exception if anything goes wrong in the process
     */
    @Trace
    @Override
    public Optional<List<String>> listFiles(
            final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
            String entityId, String connectorName) throws FileTransferException {

        final Optional<List<String>> fileListFullName = s3OperationsManager.listFilesInFolder(optionsObjectMap, false, entityId, connectorName);
        final String remoteDir = (String)optionsObjectMap.get(FileSystemOptionKeys.REMOTE_DIR);
        List<String> fileList = new ArrayList<>();
        if(fileListFullName.isPresent()) {
            for (final String name: fileListFullName.get()) {
                final String objName = s3OperationsManager.normalizeFileName(name.substring(remoteDir.length()));
                if(!objName.equalsIgnoreCase("")) {
                    fileList.add(objName);
                }
            }
        } else {
            logger.warn(ErrorConstants.FOLDER_EMPTY);
        }
        return Optional.of(fileList);
    }

    /**
     * Rename a give file if present.
     * @param fullFileName
     * @param optionsObjectMap
     * @param entityId
     * @param connectorName
     * @return
     * @throws GISTFileTransferExceptions
     */
    @Trace
    @Override
    public Optional<FileOperationResponse> renameFile(
            final String fullFileName, Map<FileSystemOptionKeys, Object> optionsObjectMap, 
            String entityId, String connectorName) throws FileTransferException {
        return Optional.empty();
    }

    /**
     * Create a directory in the remote location if the directory is not present.
     * @param createDirectoryPayload
     * @param optionsObjectMap
     * @param entityId
     * @param connectorName
     * @return
     * @throws GISTFileTransferExceptions
     */
    @Trace
    @Override
    public Optional<String> createDirectory(final CreateDirectoryPayload createDirectoryPayload,
                                            final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
                                            String entityId, String connectorName) throws FileTransferException {
        return s3OperationsManager.createdDir(createDirectoryPayload, optionsObjectMap, entityId, connectorName);
    }

    /**
     * Remove the remote directory. Delete it if this is present.
     * @param deleteFolderPayLoad
     * @param optionsObjectMap
     * @param entityId
     * @param connectorName
     * @return
     * @throws GISTFileTransferExceptions
     */
    @Trace
    @Override
    public Optional<String> removeDirectory(DeleteFolderPayLoad deleteFolderPayLoad,
                                            final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
                                            String entityId, String connectorName) throws FileTransferException {
        return s3OperationsManager.removeDir(deleteFolderPayLoad, optionsObjectMap, entityId, connectorName);
    }

    /**
     * Get the remote file as a Stream of bytes.
     * @param optionsObjectMap
     * @param entityId
     * @param connectorName
     * @return
     * @throws GISTFileTransferExceptions
     */
    @Trace
    @Override
    public Optional<byte[]> getRemoteFileStream(
            final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
            String entityId, String connectorName) throws FileTransferException {
        final Optional<FileObjectProxy> fileObjectProxyOptional = getRemoteFileStreamV2(optionsObjectMap, entityId, connectorName);
        Optional<byte[]> result = Optional.empty();
        FileObjectProxy fileObjectProxy = null;
        try {
            if(fileObjectProxyOptional.isPresent()) {
                fileObjectProxy = fileObjectProxyOptional.get();
                result = Optional.of(IOUtils.toByteArray(fileObjectProxy.getFileStream()));
            } else {
                logger.error(String.format("getRemoteFileStream: %s",ErrorConstants.CANNOT_FETCH_FILE));
            }
        } catch (IOException e) {
            logger.error(String.format("getRemoteFileStream: %s", e));
            throw new CannotFetchRemoteFileException(e.getMessage());
        } finally {
            if(fileObjectProxy != null) {
                fileObjectProxy.close();
            }
        }
        return result;
    }

    /**
     * Get the remote file as a stream.
     * @param optionsObjectMap
     * @param entityId
     * @param connectorName
     * @return
     * @throws GISTFileTransferExceptions
     */
    @Trace
    @Override
    public Optional<FileObjectProxy> getRemoteFileStreamV2(
            final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
            String entityId, String connectorName) throws FileTransferException {
        final String remoteDir = ((String) optionsObjectMap.get(FileSystemOptionKeys.REMOTE_DIR))
                .replace("\\", "/");
        final String fileName = ((String) optionsObjectMap.get(FileSystemOptionKeys.FILE_NAME))
        			.replace("\\", "/");
        optionsObjectMap.put(FileSystemOptionKeys.FILE_NAME, fileName);
        optionsObjectMap.put(FileSystemOptionKeys.REMOTE_DIR, remoteDir);
        if(remoteDir.isEmpty() || fileName.isEmpty()) {
            final String w = ErrorConstants.INVALID_FILE_NAME + ErrorConstants.DISPLAY_SPACE + remoteDir + "/" + fileName;
            logger.error(w);
            throw new InvalidFileNamePatternException(w);
        }
        final Optional<FileObjectProxy> fileObjectProxyOptional =
                s3OperationsManager.getFileInputStream(optionsObjectMap, entityId, connectorName);

        return fileObjectProxyOptional;
    }

    /**
     * Put the file as a remote file stream.
     * @param optionsObjectMap
     * @param entityId
     * @param connectorName
     * @return
     * @throws GISTFileTransferExceptions
     */
    @Trace
    @Override
    public Optional<FileOperationResponse> setRemoteFileStream(
            final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
            String entityId, String connectorName) throws FileTransferException {
        return setRemoteFileStreamV2(optionsObjectMap, entityId, connectorName);
    }

    /**
     * Put the file as a remote file stream.
     * @param optionsObjectMap
     * @param entityId
     * @param connectorName
     * @return
     * @throws GISTFileTransferExceptions
     */
    @Trace
    @Override
    public Optional<FileOperationResponse> setRemoteFileStreamV2(
            final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
            String entityId, String connectorName) throws FileTransferException {
        final String remoteDir = ((String) optionsObjectMap.get(FileSystemOptionKeys.REMOTE_DIR))
                .replace("\\", "/");
        final String fileName = ((String) optionsObjectMap.get(FileSystemOptionKeys.FILE_NAME))
                .replace("\\", "/");
        optionsObjectMap.put(FileSystemOptionKeys.FILE_NAME, fileName);
        optionsObjectMap.put(FileSystemOptionKeys.REMOTE_DIR, remoteDir);
        if(remoteDir.isEmpty() || fileName.isEmpty()) {
            logger.error(ErrorConstants.INVALID_FILE_NAME + ErrorConstants.DISPLAY_SPACE + remoteDir + "/" + fileName);
            throw new InvalidFileNamePatternException(
            		ErrorConstants.INVALID_FILE_NAME + ErrorConstants.DISPLAY_SPACE + remoteDir + "/" + fileName);
        }

        return s3OperationsManager.setFileStream(optionsObjectMap, entityId, connectorName);
    }

    /**
     * Move the given file from one location to another.
     * @param moveFilePayload
     * @param optionsObjectMap
     * @param entityId
     * @param connectorName
     * @return
     * @throws GISTFileTransferExceptions
     */
    @Trace
    @Override
    public Optional<String> moveFilesV1(final MoveFilePayload moveFilePayload,
                                        final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
                                        String entityId, String connectorName) throws FileTransferException {
        return s3OperationsManager.moveFile(moveFilePayload, optionsObjectMap, entityId, connectorName, true);
    }

    /**
     * Move the file from one location to another.
     * @param moveFilePayload
     * @param optionsObjectMap
     * @param entityId
     * @param connectorName
     * @return
     * @throws GISTFileTransferExceptions
     */
    @Trace
    @Override
    public Optional<String> moveFilesV2(final MoveFilePayload moveFilePayload,
                                        final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
                                        String entityId, String connectorName) throws FileTransferException {
        return s3OperationsManager.moveFile(moveFilePayload, optionsObjectMap, entityId, connectorName, true);
    }

    /**
     * Move the file from one location to another.
     * @param moveFilePayload
     * @param optionsObjectMap
     * @param entityId
     * @param connectorName
     * @return
     * @throws GISTFileTransferExceptions
     */
    @Trace
    @Override
    public Optional<String> copyFilesV1(final MoveFilePayload moveFilePayload,
                                        final Map<FileSystemOptionKeys, Object> optionsObjectMap,
                                        String entityId, String connectorName) throws FileTransferException {
        return s3OperationsManager.moveFile(moveFilePayload, optionsObjectMap, entityId, connectorName, false);
    }

    /**
     * Test certain features of the S3.
     * @param env
     * @return
     */
    @Trace
    @Override
    public String testFileOperations(final String env) {
        return s3OperationsManager.testS3Access(env);
    }
}
