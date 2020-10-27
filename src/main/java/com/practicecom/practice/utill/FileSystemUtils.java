package com.practicecom.practice.utill;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import org.springframework.util.StringUtils;

import com.practice.exception.FileTransferException;

import static com.practice.constants.ConfigurationConsts.SUCCESS;
import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class FileSystemUtils {
    private FileSystemUtils(){}

    public static String deleteFolder(final Path path) throws FileTransferException {
        final StringBuilder str = new StringBuilder();
        if (path.toFile().exists()) {
            try (final Stream<Path> walk = Files.walk(path)) {
                walk.parallel()
                        .sorted(Comparator.reverseOrder())
                        .forEachOrdered(
                                f -> {
                                    try {
                                        delete(f);
                                    } catch (FileTransferException e) {
                                        final String msg = format("Delete file exception: %s",e.getMessage());
                                        str.append(msg);
                                    }
                                }
                        );
            } catch (Exception e) {
                throw new FileTransferException(e.getMessage());
            }
        } else {
            final String msg = format("%s: Folder does not exist. %s", "deleteFolder", path);
            throw new FileTransferException(msg);
        }
        return str.toString().isEmpty() ? SUCCESS : str.toString();
    }

    public static Path moveFolder(final Path source, final Path target) throws FileTransferException {
        Path movedTarget;
        if (source.toFile().exists()) {
            try {
                if (target.toFile().exists()) {
                    deleteFolder(target);
                }
                movedTarget = Files.move(source, target, REPLACE_EXISTING);
            } catch (Exception e) {
                throw new FileTransferException(e.getMessage());
            }
        } else {
            throw new FileTransferException(String.format("Source folder %s does not exist", source.toString()));
        }

        return movedTarget;
    }

    private static void delete(final Path f) throws FileTransferException {
        try {
            Files.delete(f);
        } catch (IOException e) {
            final String msg =
                    format("deleteFolder: Could not delete file: %s with IOException: %s", f, e);
            throw new FileTransferException(msg);
        }
    }

    public static Path writeContentsToFile(final Path fullfilePath, final String content) throws FileTransferException {
        Path writtenFile;
        if (fullfilePath != null && !StringUtils.isEmpty(content)) {
            try {
                final Path fileParent = fullfilePath.getParent();
                if(!fileParent.toFile().exists()) {
                    fileParent.toFile().mkdirs();
                }
                writtenFile = Files.write(fullfilePath, content.getBytes());
            } catch (Exception e) {
                final String msg = String.format("Cannot write to file %s with %s ", fullfilePath, e);
                throw new FileTransferException(msg);
            }
        } else {
            final String msg = String.format("Path or content empty. Nothing written for file: %s", fullfilePath);
            throw new FileTransferException(msg);
        }

        return writtenFile;
    }

    public static String readFileToString(final Path fullFilePath)  throws FileTransferException {
        String content;
        if (fullFilePath.toFile().exists()) {
            try {
                content = new String(Files.readAllBytes(fullFilePath));
            } catch (Exception e) {
                throw new FileTransferException(e.getMessage());
            }
        } else {
            final String msg = String.format("File %s does not exist", fullFilePath);
            throw new FileTransferException(msg);
        }

        return content;
    }
}
