package com.example.demo.config;

import io.minio.MinioClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ArtifactStorageConfig {

    @Value("${helios.artifacts.endpoint}")
    private String endpoint;

    @Value("${helios.artifacts.access-key:}")
    private String accessKey;

    @Value("${helios.artifacts.secret-key:}")
    private String secretKey;

    @Bean
    public MinioClient minioClient() {
        return MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();
    }
}
