package com.microsoft.azure.kusto.kafka.connect.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.zip.GZIPOutputStream;

/**
 * This class is used to write gzipped rolling files.
 * Currently supports size based rolling, where size is for *uncompressed* size,
 * so final size can vary.
 */
public class FileWriter implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KustoSinkTask.class);
    SourceFile currentFile;
    private Timer timer;
    private Consumer<SourceFile> onRollCallback;
    private final long flushInterval;
    private final boolean shouldCompressData;
    private Supplier<String> getFilePath;
    private OutputStream outputStream;
    private String basePath;
    private CountingOutputStream countingStream;
    private long fileThreshold;

    // Don't remove! File descriptor is kept so that the file is not deleted when stream is closed
    private FileDescriptor currentFileDescriptor;

    /**
     * @param basePath       - This is path to which to write the files to.
     * @param fileThreshold  - Max size, uncompressed bytes.
     * @param onRollCallback - Callback to allow code to execute when rolling a file. Blocking code.
     * @param getFilePath    - Allow external resolving of file name.
     * @param shouldCompressData - Should the FileWriter compress the incoming data
     */
    public FileWriter(String basePath,
                      long fileThreshold,
                      Consumer<SourceFile> onRollCallback,
                      Supplier<String> getFilePath,
                      long flushInterval,
                      boolean shouldCompressData) {
        this.getFilePath = getFilePath;
        this.basePath = basePath;
        this.fileThreshold = fileThreshold;
        this.onRollCallback = onRollCallback;
        this.flushInterval = flushInterval;
        this.shouldCompressData = shouldCompressData;
    }

    boolean isDirty() {
        return this.currentFile != null && this.currentFile.rawBytes > 0;
    }

    public synchronized void write(byte[] data) throws IOException {
        if (data == null || data.length == 0) return;

        if (currentFile == null) {
            openFile();
            resetFlushTimer(true);
        }

        outputStream.write(data);

        currentFile.rawBytes += data.length;
        currentFile.zippedBytes += countingStream.numBytes;
        currentFile.numRecords++;

        if (this.flushInterval == 0 || currentFile.rawBytes > fileThreshold) {
            rotate();
            resetFlushTimer(true);
        }
    }

    public void openFile() throws IOException {
        SourceFile fileProps = new SourceFile();

        File folder = new File(basePath);
        if (!folder.exists() && !folder.mkdirs()) {
            throw new IOException(String.format("Failed to create new directory %s", folder.getPath()));
        }

        String filePath = getFilePath.get();
        fileProps.path = filePath;

        File file = new File(filePath);

        file.createNewFile();

        FileOutputStream fos = new FileOutputStream(file);
        currentFileDescriptor = fos.getFD();
        fos.getChannel().truncate(0);

        countingStream = new CountingOutputStream(fos);
        outputStream = shouldCompressData ? new GZIPOutputStream(countingStream) : countingStream;
        fileProps.file = file;
        currentFile = fileProps;
    }

    void rotate() throws IOException {
        finishFile(true);
        openFile();
    }

    void finishFile(Boolean delete) throws IOException {
        if(isDirty()){
            if(shouldCompressData){
                GZIPOutputStream gzip = (GZIPOutputStream) outputStream;
                gzip.finish();
            } else {
                outputStream.flush();
            }

            onRollCallback.accept(currentFile);
            if (delete){
                dumpFile();
            }
        } else {
            outputStream.close();
        }
    }

    private void dumpFile() throws IOException {
        outputStream.close();
        currentFileDescriptor = null;
        boolean deleted = currentFile.file.delete();
        if (!deleted) {
            log.warn("couldn't delete temporary file. File exists: " + currentFile.file.exists());
        }
    }

    public void rollback() throws IOException {
        if (outputStream != null) {
            outputStream.close();
            if (currentFile != null && currentFile.file != null) {
                dumpFile();
            }
        }
    }

    public void close() throws IOException {
        if (timer!= null) {
            timer.cancel();
            timer.purge();
        }

        // Flush last file, updating index
        finishFile(true);

        // Setting to null so subsequent calls to close won't write it again
        currentFile = null;
    }

    // Set shouldDestroyTimer to true if the current running task should be cancelled
    private void resetFlushTimer(Boolean shouldDestroyTimer) {
        if (flushInterval > 0) {
            if (shouldDestroyTimer) {
                if (timer != null) {
                    timer.purge();
                    timer.cancel();
                }

                timer = new Timer(true);
            }

            TimerTask t = new TimerTask() {
                @Override
                public void run() {
                    flushByTimeImpl();
                }
            };
            timer.schedule(t, flushInterval);
        }
    }

    private void flushByTimeImpl() {
        try {
            if (currentFile != null && currentFile.rawBytes > 0) {
                rotate();
            }
        } catch (Exception e) {
            String fileName = currentFile == null ? "no file created yet" : currentFile.file.getName();
            long currentSize = currentFile == null ? 0 : currentFile.rawBytes;
            log.error(String.format("Error in flushByTime. Current file: %s, size: %d. ", fileName, currentSize), e);
        }

        resetFlushTimer(false);
    }

    private class CountingOutputStream extends FilterOutputStream {
        private long numBytes = 0;

        CountingOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            this.numBytes++;
        }

        @Override
        public void write(byte[] b) throws IOException {
            out.write(b);
            this.numBytes += b.length;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            this.numBytes += len;
        }
    }
}

