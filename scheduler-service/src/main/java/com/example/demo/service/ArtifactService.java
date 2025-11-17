package com.example.demo.service;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.GetObjectArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

@Service
public class ArtifactService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ArtifactService.class);

    private final MinioClient minioClient;
    private final String bucket;

    public ArtifactService(MinioClient minioClient,
                           @Value("${helios.artifacts.bucket}") String bucket) {
        this.minioClient = minioClient;
        this.bucket = bucket;
        ensureBucket();
    }

    private void ensureBucket() {
        try {
            boolean exists = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucket).build());
            if (!exists) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
                LOGGER.info("Created MinIO bucket '{}'", bucket);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to verify/create bucket '{}': {}", bucket, e.getMessage());
        }
    }

    public void upload(String key, InputStream data, long size, String contentType) throws Exception {
        int maxRetries = 3;
        long backoffMillis = 500;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                minioClient.putObject(
                        PutObjectArgs.builder()
                                .bucket(bucket)
                                .object(key)
                                .stream(data, size, -1)
                                .contentType(contentType != null ? contentType : "application/octet-stream")
                                .build()
                );
                return;
            } catch (Exception e) {
                if (attempt == maxRetries) {
                    throw e;
                }
                LOGGER.warn("Upload failed for key '{}' (attempt {}/{}): {}", key, attempt, maxRetries, e.getMessage());
                try {
                    TimeUnit.MILLISECONDS.sleep(backoffMillis);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                backoffMillis *= 2;
            }
        }
    }

    public InputStream download(String key) throws Exception {
        int maxRetries = 3;
        long backoffMillis = 500;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return minioClient.getObject(GetObjectArgs.builder()
                        .bucket(bucket)
                        .object(key)
                        .build());
            } catch (Exception e) {
                if (attempt == maxRetries) {
                    throw e;
                }
                LOGGER.warn("Download failed for key '{}' (attempt {}/{}): {}", key, attempt, maxRetries, e.getMessage());
                try {
                    TimeUnit.MILLISECONDS.sleep(backoffMillis);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                backoffMillis *= 2;
            }
        }
        throw new IllegalStateException("Unreachable");
    }
}

