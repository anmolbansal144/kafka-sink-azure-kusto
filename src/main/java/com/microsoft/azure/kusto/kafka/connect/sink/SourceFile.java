package com.microsoft.azure.kusto.kafka.connect.sink;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class SourceFile {
    long rawBytes = 0;
    long zippedBytes = 0;
    long numRecords = 0;
    public String path;
    public File file;
    public List<byte[]> records = new ArrayList<>();
}