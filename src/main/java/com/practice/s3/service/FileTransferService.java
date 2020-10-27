package com.practice.s3.service;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.practice.constants.FileSystemOptionKeys;
import com.practice.exception.FileTransferException;
import com.practice.model.CreateDirectoryPayload;
import com.practice.model.DeleteFolderPayLoad;
import com.practice.model.FileObjectProxy;
import com.practice.model.FileOperationResponse;
import com.practice.model.MoveFilePayload;
import com.practice.model.Pair;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public interface FileTransferService {
    Optional<FileOperationResponse> uploadFile(final MultipartFile multipartFile, final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
            String entityId, String connectorName) throws FileTransferException;
    Optional<Pair<FileOperationResponse, String>> downloadFile(final String fullFileName, final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
            String entityId, String connectorName) throws FileTransferException;
    Optional<List<String>> listFiles(final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
            String entityId, String connectorName) throws FileTransferException;
    Optional<FileOperationResponse> renameFile(final String fullFileName, final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
                                        String entityId, String connectorName) throws FileTransferException;
    Optional<String> createDirectory(final CreateDirectoryPayload createDirectoryPayload, final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
                                        String entityId, String connectorName) throws FileTransferException;
    Optional<String> removeDirectory(final DeleteFolderPayLoad deleteFolderPayLoad, final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
                                        String entityId, String connectorName) throws FileTransferException;
    Optional<byte[]> getRemoteFileStream(final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
                                        String entityId, String connectorName) throws FileTransferException;
    Optional<FileObjectProxy> getRemoteFileStreamV2(final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
                                        String entityId, String connectorName) throws FileTransferException;
    Optional<FileOperationResponse> setRemoteFileStream(final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
                                        String entityId, String connectorName) throws FileTransferException;
    Optional<FileOperationResponse> setRemoteFileStreamV2(final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
                                        String entityId, String connectorName) throws FileTransferException;
    Optional<String> moveFilesV1(final MoveFilePayload moveFilePayload, final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
                                        String entityId, String connectorName) throws FileTransferException;
    Optional<String> moveFilesV2(final MoveFilePayload moveFilePayload, final Map<FileSystemOptionKeys, Object> optionsObjectMap, 
                                        String entityId, String connectorName) throws FileTransferException;
    Optional<String> copyFilesV1(final MoveFilePayload moveFilePayload, final Map<FileSystemOptionKeys, Object> optionsObjectMap,
                                 String entityId, String connectorName) throws FileTransferException;
    String testFileOperations(final String env);
}
