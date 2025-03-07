# Sidecar
Sidecar is the Hadoop - compatible **caching (read/write)** file system. It was specifically designed to support faster read/write access to remote cloud storage systems. 

## Supported file systems

* `file`  - local file system (for testing only)
* `hdfs`  - Hadoop Distributed File System.
* `s3a`   - AWS S3
* `adl`   - Azure Data Lake File System Gen 1
* `abfs`  - ABFS Azure Data Lake Gen 2
* `abfss` - Secure ABFSS Azure Data Lake Gen 2
* `wasb`  - WASB native Azure File System
* `wasbs` - Secure WASB native Azure FS
* `gs`    - Google Cloud Storage
* `oss`   - Chineese Aliyun OSS (Alibaba Cloud)
* `swift` - Open Stack Swift Object Store

## How to use
You will need to modify Hadoop configuration file to use a caching version of a remote file system.
### Local File System

```bash
fs.file.impl=com.carrotdata.sidecar.fs.file.FileSidecarCachingFileSystem
```

### Hadoop Distributed File System

```bash
fs.hdfs.impl=com.carrotdata.sidecar.hdfs.SidecarDistributedFileSystem
```

### AWS S3

```bash
fs.s3a.impl=com.carrotdata.sidecar.s3a.SidecarS3AFileSystem
```

### Azure Data Lake File System Gen 1

```bash
fs.adl.impl=com.carrotdata.sidecar.adl.SidecarAdlFileSystem
```

### Azure Data Lake File System Gen 2

```bash
fs.abfs.impl=com.carrotdata.sidecar.abfs.SidecarAzureBlobFileSystem
```

### Azure Data Lake File System Gen 2 (Secure)

```bash
fs.abfss.impl=com.carrotdata.sidecar.abfs.SecureSidecarAzureBlobFileSystem
```

### WASB native Azure File System

```bash
fs.wasb.impl=com.carrotdata.sidecar.wasb.SidecarNativeAzureBlobFileSystem
```

### WASB native Azure File System (Secure)

```bash
fs.wasbs.impl=com.carrotdata.sidecar.wasb.SecureSidecarNativeAzureBlobFileSystem
```

### Google Cloud Storage

```bash
fs.gs.impl=com.carrotdata.sidecar.gcs.SidecarGoogleHadoopFileSystem
```

### Chineese Aliyun OSS (Alibaba Cloud)

```bash
fs.oss.impl=com.carrotdata.sidecar.oss.SidecarAliyunOSSFileSystem
```

### Chineese Aliyun OSS (Alibaba Cloud)

```bash
fs.oss.impl=com.carrotdata.sidecar.oss.SidecarAliyunOSSFileSystem
```

### OpenStack Swift Object Store

```bash
fs.swift.impl=com.carrotdata.sidecar.swift.SidecarSwiftNativeFileSystem
```









