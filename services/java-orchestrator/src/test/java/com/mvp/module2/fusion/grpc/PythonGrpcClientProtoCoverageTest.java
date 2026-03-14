package com.mvp.module2.fusion.grpc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PythonGrpcClientProtoCoverageTest {

    @TempDir
    Path tempDir;

    @Test
    void selfCheckCoverageShouldIncludeTranscribeRequest() {
        assertTrue(PythonGrpcClient.requiredGrpcProtoMessageSimpleNamesForSelfCheck().contains("TranscribeRequest"));
    }

    @Test
    void verifyGeneratedFilesShouldRequireTranscribeRequestJava() throws Exception {
        Path generatedDir = tempDir
            .resolve("services")
            .resolve("java-orchestrator")
            .resolve("target")
            .resolve("generated-sources")
            .resolve("protobuf")
            .resolve("java")
            .resolve("com")
            .resolve("mvp")
            .resolve("videoprocessing")
            .resolve("grpc");
        Files.createDirectories(generatedDir);
        for (String fileName : PythonGrpcClient.requiredGrpcProtoGeneratedMessageFiles()) {
            if (!"TranscribeRequest.java".equals(fileName)) {
                Files.createFile(generatedDir.resolve(fileName));
            }
        }

        Path grpcDir = tempDir
            .resolve("services")
            .resolve("java-orchestrator")
            .resolve("target")
            .resolve("generated-sources")
            .resolve("protobuf")
            .resolve("grpc-java")
            .resolve("com")
            .resolve("mvp")
            .resolve("videoprocessing")
            .resolve("grpc");
        Files.createDirectories(grpcDir);
        Files.createFile(grpcDir.resolve("VideoProcessingServiceGrpc.java"));

        IllegalStateException error = assertThrows(
            IllegalStateException.class,
            () -> invokePrivatePathMethod("verifyGrpcProtoGeneratedFiles", tempDir)
        );
        assertTrue(error.getMessage().contains("TranscribeRequest.java"));
    }

    @Test
    void verifyCompiledClassesShouldRequireTranscribeRequestClass() throws Exception {
        Path classesDir = tempDir
            .resolve("services")
            .resolve("java-orchestrator")
            .resolve("target")
            .resolve("classes")
            .resolve("com")
            .resolve("mvp")
            .resolve("videoprocessing")
            .resolve("grpc");
        Files.createDirectories(classesDir);
        for (String fileName : PythonGrpcClient.requiredGrpcProtoCompiledClassFiles()) {
            if (!"TranscribeRequest.class".equals(fileName)) {
                Files.createFile(classesDir.resolve(fileName));
            }
        }

        IllegalStateException error = assertThrows(
            IllegalStateException.class,
            () -> invokePrivatePathMethod("verifyGrpcProtoCompiledClasses", tempDir)
        );
        assertTrue(error.getMessage().contains("TranscribeRequest.class"));
    }

    private static void invokePrivatePathMethod(String methodName, Path repoRoot) throws Exception {
        Method method = PythonGrpcClient.class.getDeclaredMethod(methodName, Path.class);
        method.setAccessible(true);
        try {
            method.invoke(new PythonGrpcClient(), repoRoot);
        } catch (InvocationTargetException error) {
            Throwable cause = error.getCause();
            if (cause instanceof Exception checked) {
                throw checked;
            }
            if (cause instanceof Error fatal) {
                throw fatal;
            }
            throw new IllegalStateException("Unexpected reflection failure", cause);
        }
    }
}
