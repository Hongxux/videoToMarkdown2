package com.mvp.videoprocessing.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.63.0)",
    comments = "Source: video_processing.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class VideoProcessingServiceGrpc {

  private VideoProcessingServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "videoprocessing.VideoProcessingService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.DownloadRequest,
      com.mvp.videoprocessing.grpc.DownloadResponse> getDownloadVideoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DownloadVideo",
      requestType = com.mvp.videoprocessing.grpc.DownloadRequest.class,
      responseType = com.mvp.videoprocessing.grpc.DownloadResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.DownloadRequest,
      com.mvp.videoprocessing.grpc.DownloadResponse> getDownloadVideoMethod() {
    io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.DownloadRequest, com.mvp.videoprocessing.grpc.DownloadResponse> getDownloadVideoMethod;
    if ((getDownloadVideoMethod = VideoProcessingServiceGrpc.getDownloadVideoMethod) == null) {
      synchronized (VideoProcessingServiceGrpc.class) {
        if ((getDownloadVideoMethod = VideoProcessingServiceGrpc.getDownloadVideoMethod) == null) {
          VideoProcessingServiceGrpc.getDownloadVideoMethod = getDownloadVideoMethod =
              io.grpc.MethodDescriptor.<com.mvp.videoprocessing.grpc.DownloadRequest, com.mvp.videoprocessing.grpc.DownloadResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DownloadVideo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.DownloadRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.DownloadResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VideoProcessingServiceMethodDescriptorSupplier("DownloadVideo"))
              .build();
        }
      }
    }
    return getDownloadVideoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.TranscribeRequest,
      com.mvp.videoprocessing.grpc.TranscribeResponse> getTranscribeVideoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "TranscribeVideo",
      requestType = com.mvp.videoprocessing.grpc.TranscribeRequest.class,
      responseType = com.mvp.videoprocessing.grpc.TranscribeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.TranscribeRequest,
      com.mvp.videoprocessing.grpc.TranscribeResponse> getTranscribeVideoMethod() {
    io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.TranscribeRequest, com.mvp.videoprocessing.grpc.TranscribeResponse> getTranscribeVideoMethod;
    if ((getTranscribeVideoMethod = VideoProcessingServiceGrpc.getTranscribeVideoMethod) == null) {
      synchronized (VideoProcessingServiceGrpc.class) {
        if ((getTranscribeVideoMethod = VideoProcessingServiceGrpc.getTranscribeVideoMethod) == null) {
          VideoProcessingServiceGrpc.getTranscribeVideoMethod = getTranscribeVideoMethod =
              io.grpc.MethodDescriptor.<com.mvp.videoprocessing.grpc.TranscribeRequest, com.mvp.videoprocessing.grpc.TranscribeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "TranscribeVideo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.TranscribeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.TranscribeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VideoProcessingServiceMethodDescriptorSupplier("TranscribeVideo"))
              .build();
        }
      }
    }
    return getTranscribeVideoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.Stage1Request,
      com.mvp.videoprocessing.grpc.Stage1Response> getProcessStage1Method;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ProcessStage1",
      requestType = com.mvp.videoprocessing.grpc.Stage1Request.class,
      responseType = com.mvp.videoprocessing.grpc.Stage1Response.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.Stage1Request,
      com.mvp.videoprocessing.grpc.Stage1Response> getProcessStage1Method() {
    io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.Stage1Request, com.mvp.videoprocessing.grpc.Stage1Response> getProcessStage1Method;
    if ((getProcessStage1Method = VideoProcessingServiceGrpc.getProcessStage1Method) == null) {
      synchronized (VideoProcessingServiceGrpc.class) {
        if ((getProcessStage1Method = VideoProcessingServiceGrpc.getProcessStage1Method) == null) {
          VideoProcessingServiceGrpc.getProcessStage1Method = getProcessStage1Method =
              io.grpc.MethodDescriptor.<com.mvp.videoprocessing.grpc.Stage1Request, com.mvp.videoprocessing.grpc.Stage1Response>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ProcessStage1"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.Stage1Request.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.Stage1Response.getDefaultInstance()))
              .setSchemaDescriptor(new VideoProcessingServiceMethodDescriptorSupplier("ProcessStage1"))
              .build();
        }
      }
    }
    return getProcessStage1Method;
  }

  private static volatile io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.AnalyzeRequest,
      com.mvp.videoprocessing.grpc.AnalyzeResponse> getAnalyzeSemanticUnitsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AnalyzeSemanticUnits",
      requestType = com.mvp.videoprocessing.grpc.AnalyzeRequest.class,
      responseType = com.mvp.videoprocessing.grpc.AnalyzeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.AnalyzeRequest,
      com.mvp.videoprocessing.grpc.AnalyzeResponse> getAnalyzeSemanticUnitsMethod() {
    io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.AnalyzeRequest, com.mvp.videoprocessing.grpc.AnalyzeResponse> getAnalyzeSemanticUnitsMethod;
    if ((getAnalyzeSemanticUnitsMethod = VideoProcessingServiceGrpc.getAnalyzeSemanticUnitsMethod) == null) {
      synchronized (VideoProcessingServiceGrpc.class) {
        if ((getAnalyzeSemanticUnitsMethod = VideoProcessingServiceGrpc.getAnalyzeSemanticUnitsMethod) == null) {
          VideoProcessingServiceGrpc.getAnalyzeSemanticUnitsMethod = getAnalyzeSemanticUnitsMethod =
              io.grpc.MethodDescriptor.<com.mvp.videoprocessing.grpc.AnalyzeRequest, com.mvp.videoprocessing.grpc.AnalyzeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AnalyzeSemanticUnits"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.AnalyzeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.AnalyzeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VideoProcessingServiceMethodDescriptorSupplier("AnalyzeSemanticUnits"))
              .build();
        }
      }
    }
    return getAnalyzeSemanticUnitsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.AssembleRequest,
      com.mvp.videoprocessing.grpc.AssembleResponse> getAssembleRichTextMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AssembleRichText",
      requestType = com.mvp.videoprocessing.grpc.AssembleRequest.class,
      responseType = com.mvp.videoprocessing.grpc.AssembleResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.AssembleRequest,
      com.mvp.videoprocessing.grpc.AssembleResponse> getAssembleRichTextMethod() {
    io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.AssembleRequest, com.mvp.videoprocessing.grpc.AssembleResponse> getAssembleRichTextMethod;
    if ((getAssembleRichTextMethod = VideoProcessingServiceGrpc.getAssembleRichTextMethod) == null) {
      synchronized (VideoProcessingServiceGrpc.class) {
        if ((getAssembleRichTextMethod = VideoProcessingServiceGrpc.getAssembleRichTextMethod) == null) {
          VideoProcessingServiceGrpc.getAssembleRichTextMethod = getAssembleRichTextMethod =
              io.grpc.MethodDescriptor.<com.mvp.videoprocessing.grpc.AssembleRequest, com.mvp.videoprocessing.grpc.AssembleResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AssembleRichText"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.AssembleRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.AssembleResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VideoProcessingServiceMethodDescriptorSupplier("AssembleRichText"))
              .build();
        }
      }
    }
    return getAssembleRichTextMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.CVValidationRequest,
      com.mvp.videoprocessing.grpc.CVValidationResponse> getValidateCVBatchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ValidateCVBatch",
      requestType = com.mvp.videoprocessing.grpc.CVValidationRequest.class,
      responseType = com.mvp.videoprocessing.grpc.CVValidationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.CVValidationRequest,
      com.mvp.videoprocessing.grpc.CVValidationResponse> getValidateCVBatchMethod() {
    io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.CVValidationRequest, com.mvp.videoprocessing.grpc.CVValidationResponse> getValidateCVBatchMethod;
    if ((getValidateCVBatchMethod = VideoProcessingServiceGrpc.getValidateCVBatchMethod) == null) {
      synchronized (VideoProcessingServiceGrpc.class) {
        if ((getValidateCVBatchMethod = VideoProcessingServiceGrpc.getValidateCVBatchMethod) == null) {
          VideoProcessingServiceGrpc.getValidateCVBatchMethod = getValidateCVBatchMethod =
              io.grpc.MethodDescriptor.<com.mvp.videoprocessing.grpc.CVValidationRequest, com.mvp.videoprocessing.grpc.CVValidationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ValidateCVBatch"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.CVValidationRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.CVValidationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VideoProcessingServiceMethodDescriptorSupplier("ValidateCVBatch"))
              .build();
        }
      }
    }
    return getValidateCVBatchMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.KnowledgeClassificationRequest,
      com.mvp.videoprocessing.grpc.KnowledgeClassificationResponse> getClassifyKnowledgeBatchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ClassifyKnowledgeBatch",
      requestType = com.mvp.videoprocessing.grpc.KnowledgeClassificationRequest.class,
      responseType = com.mvp.videoprocessing.grpc.KnowledgeClassificationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.KnowledgeClassificationRequest,
      com.mvp.videoprocessing.grpc.KnowledgeClassificationResponse> getClassifyKnowledgeBatchMethod() {
    io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.KnowledgeClassificationRequest, com.mvp.videoprocessing.grpc.KnowledgeClassificationResponse> getClassifyKnowledgeBatchMethod;
    if ((getClassifyKnowledgeBatchMethod = VideoProcessingServiceGrpc.getClassifyKnowledgeBatchMethod) == null) {
      synchronized (VideoProcessingServiceGrpc.class) {
        if ((getClassifyKnowledgeBatchMethod = VideoProcessingServiceGrpc.getClassifyKnowledgeBatchMethod) == null) {
          VideoProcessingServiceGrpc.getClassifyKnowledgeBatchMethod = getClassifyKnowledgeBatchMethod =
              io.grpc.MethodDescriptor.<com.mvp.videoprocessing.grpc.KnowledgeClassificationRequest, com.mvp.videoprocessing.grpc.KnowledgeClassificationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ClassifyKnowledgeBatch"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.KnowledgeClassificationRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.KnowledgeClassificationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VideoProcessingServiceMethodDescriptorSupplier("ClassifyKnowledgeBatch"))
              .build();
        }
      }
    }
    return getClassifyKnowledgeBatchMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.GenerateMaterialRequestsRequest,
      com.mvp.videoprocessing.grpc.GenerateMaterialRequestsResponse> getGenerateMaterialRequestsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GenerateMaterialRequests",
      requestType = com.mvp.videoprocessing.grpc.GenerateMaterialRequestsRequest.class,
      responseType = com.mvp.videoprocessing.grpc.GenerateMaterialRequestsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.GenerateMaterialRequestsRequest,
      com.mvp.videoprocessing.grpc.GenerateMaterialRequestsResponse> getGenerateMaterialRequestsMethod() {
    io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.GenerateMaterialRequestsRequest, com.mvp.videoprocessing.grpc.GenerateMaterialRequestsResponse> getGenerateMaterialRequestsMethod;
    if ((getGenerateMaterialRequestsMethod = VideoProcessingServiceGrpc.getGenerateMaterialRequestsMethod) == null) {
      synchronized (VideoProcessingServiceGrpc.class) {
        if ((getGenerateMaterialRequestsMethod = VideoProcessingServiceGrpc.getGenerateMaterialRequestsMethod) == null) {
          VideoProcessingServiceGrpc.getGenerateMaterialRequestsMethod = getGenerateMaterialRequestsMethod =
              io.grpc.MethodDescriptor.<com.mvp.videoprocessing.grpc.GenerateMaterialRequestsRequest, com.mvp.videoprocessing.grpc.GenerateMaterialRequestsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GenerateMaterialRequests"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.GenerateMaterialRequestsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.GenerateMaterialRequestsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VideoProcessingServiceMethodDescriptorSupplier("GenerateMaterialRequests"))
              .build();
        }
      }
    }
    return getGenerateMaterialRequestsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.ReleaseResourcesRequest,
      com.mvp.videoprocessing.grpc.ReleaseResourcesResponse> getReleaseCVResourcesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReleaseCVResources",
      requestType = com.mvp.videoprocessing.grpc.ReleaseResourcesRequest.class,
      responseType = com.mvp.videoprocessing.grpc.ReleaseResourcesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.ReleaseResourcesRequest,
      com.mvp.videoprocessing.grpc.ReleaseResourcesResponse> getReleaseCVResourcesMethod() {
    io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.ReleaseResourcesRequest, com.mvp.videoprocessing.grpc.ReleaseResourcesResponse> getReleaseCVResourcesMethod;
    if ((getReleaseCVResourcesMethod = VideoProcessingServiceGrpc.getReleaseCVResourcesMethod) == null) {
      synchronized (VideoProcessingServiceGrpc.class) {
        if ((getReleaseCVResourcesMethod = VideoProcessingServiceGrpc.getReleaseCVResourcesMethod) == null) {
          VideoProcessingServiceGrpc.getReleaseCVResourcesMethod = getReleaseCVResourcesMethod =
              io.grpc.MethodDescriptor.<com.mvp.videoprocessing.grpc.ReleaseResourcesRequest, com.mvp.videoprocessing.grpc.ReleaseResourcesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReleaseCVResources"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.ReleaseResourcesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.ReleaseResourcesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VideoProcessingServiceMethodDescriptorSupplier("ReleaseCVResources"))
              .build();
        }
      }
    }
    return getReleaseCVResourcesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.HealthCheckRequest,
      com.mvp.videoprocessing.grpc.HealthCheckResponse> getHealthCheckMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "HealthCheck",
      requestType = com.mvp.videoprocessing.grpc.HealthCheckRequest.class,
      responseType = com.mvp.videoprocessing.grpc.HealthCheckResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.HealthCheckRequest,
      com.mvp.videoprocessing.grpc.HealthCheckResponse> getHealthCheckMethod() {
    io.grpc.MethodDescriptor<com.mvp.videoprocessing.grpc.HealthCheckRequest, com.mvp.videoprocessing.grpc.HealthCheckResponse> getHealthCheckMethod;
    if ((getHealthCheckMethod = VideoProcessingServiceGrpc.getHealthCheckMethod) == null) {
      synchronized (VideoProcessingServiceGrpc.class) {
        if ((getHealthCheckMethod = VideoProcessingServiceGrpc.getHealthCheckMethod) == null) {
          VideoProcessingServiceGrpc.getHealthCheckMethod = getHealthCheckMethod =
              io.grpc.MethodDescriptor.<com.mvp.videoprocessing.grpc.HealthCheckRequest, com.mvp.videoprocessing.grpc.HealthCheckResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "HealthCheck"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.HealthCheckRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.videoprocessing.grpc.HealthCheckResponse.getDefaultInstance()))
              .setSchemaDescriptor(new VideoProcessingServiceMethodDescriptorSupplier("HealthCheck"))
              .build();
        }
      }
    }
    return getHealthCheckMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static VideoProcessingServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<VideoProcessingServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<VideoProcessingServiceStub>() {
        @java.lang.Override
        public VideoProcessingServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new VideoProcessingServiceStub(channel, callOptions);
        }
      };
    return VideoProcessingServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static VideoProcessingServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<VideoProcessingServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<VideoProcessingServiceBlockingStub>() {
        @java.lang.Override
        public VideoProcessingServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new VideoProcessingServiceBlockingStub(channel, callOptions);
        }
      };
    return VideoProcessingServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static VideoProcessingServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<VideoProcessingServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<VideoProcessingServiceFutureStub>() {
        @java.lang.Override
        public VideoProcessingServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new VideoProcessingServiceFutureStub(channel, callOptions);
        }
      };
    return VideoProcessingServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     * <pre>
     * 步骤1: 下载视频 (IO密集 - Python)
     * </pre>
     */
    default void downloadVideo(com.mvp.videoprocessing.grpc.DownloadRequest request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.DownloadResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDownloadVideoMethod(), responseObserver);
    }

    /**
     * <pre>
     * 步骤2: Whisper转录 (GPU/CPU密集 - Python)
     * </pre>
     */
    default void transcribeVideo(com.mvp.videoprocessing.grpc.TranscribeRequest request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.TranscribeResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getTranscribeVideoMethod(), responseObserver);
    }

    /**
     * <pre>
     * 步骤3: Stage1处理(步骤1-6) (LLM API调用 - Python)
     * </pre>
     */
    default void processStage1(com.mvp.videoprocessing.grpc.Stage1Request request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.Stage1Response> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getProcessStage1Method(), responseObserver);
    }

    /**
     * <pre>
     * 🔑 V2: Module2 拆分为两阶段
     * 步骤4: Module2 Phase2A - 语义分析 + 时间戳提取 (不执行FFmpeg)
     * </pre>
     */
    default void analyzeSemanticUnits(com.mvp.videoprocessing.grpc.AnalyzeRequest request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.AnalyzeResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAnalyzeSemanticUnitsMethod(), responseObserver);
    }

    /**
     * <pre>
     * 步骤6: Module2 Phase2B - Vision AI验证 + 富文本组装 (使用外部截图)
     * </pre>
     */
    default void assembleRichText(com.mvp.videoprocessing.grpc.AssembleRequest request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.AssembleResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAssembleRichTextMethod(), responseObserver);
    }

    /**
     * <pre>
     * 🚀 V3: CV验证批量并行处理 (Java编排调用 - 流式返回)
     * </pre>
     */
    default void validateCVBatch(com.mvp.videoprocessing.grpc.CVValidationRequest request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.CVValidationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getValidateCVBatchMethod(), responseObserver);
    }

    /**
     */
    default void classifyKnowledgeBatch(com.mvp.videoprocessing.grpc.KnowledgeClassificationRequest request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.KnowledgeClassificationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getClassifyKnowledgeBatchMethod(), responseObserver);
    }

    /**
     */
    default void generateMaterialRequests(com.mvp.videoprocessing.grpc.GenerateMaterialRequestsRequest request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.GenerateMaterialRequestsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGenerateMaterialRequestsMethod(), responseObserver);
    }

    /**
     * <pre>
     * 🚀 V6: 资源释放
     * </pre>
     */
    default void releaseCVResources(com.mvp.videoprocessing.grpc.ReleaseResourcesRequest request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.ReleaseResourcesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReleaseCVResourcesMethod(), responseObserver);
    }

    /**
     * <pre>
     * 健康检查
     * </pre>
     */
    default void healthCheck(com.mvp.videoprocessing.grpc.HealthCheckRequest request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.HealthCheckResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getHealthCheckMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service VideoProcessingService.
   */
  public static abstract class VideoProcessingServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return VideoProcessingServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service VideoProcessingService.
   */
  public static final class VideoProcessingServiceStub
      extends io.grpc.stub.AbstractAsyncStub<VideoProcessingServiceStub> {
    private VideoProcessingServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected VideoProcessingServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new VideoProcessingServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * 步骤1: 下载视频 (IO密集 - Python)
     * </pre>
     */
    public void downloadVideo(com.mvp.videoprocessing.grpc.DownloadRequest request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.DownloadResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDownloadVideoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * 步骤2: Whisper转录 (GPU/CPU密集 - Python)
     * </pre>
     */
    public void transcribeVideo(com.mvp.videoprocessing.grpc.TranscribeRequest request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.TranscribeResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getTranscribeVideoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * 步骤3: Stage1处理(步骤1-6) (LLM API调用 - Python)
     * </pre>
     */
    public void processStage1(com.mvp.videoprocessing.grpc.Stage1Request request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.Stage1Response> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getProcessStage1Method(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * 🔑 V2: Module2 拆分为两阶段
     * 步骤4: Module2 Phase2A - 语义分析 + 时间戳提取 (不执行FFmpeg)
     * </pre>
     */
    public void analyzeSemanticUnits(com.mvp.videoprocessing.grpc.AnalyzeRequest request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.AnalyzeResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAnalyzeSemanticUnitsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * 步骤6: Module2 Phase2B - Vision AI验证 + 富文本组装 (使用外部截图)
     * </pre>
     */
    public void assembleRichText(com.mvp.videoprocessing.grpc.AssembleRequest request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.AssembleResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAssembleRichTextMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * 🚀 V3: CV验证批量并行处理 (Java编排调用 - 流式返回)
     * </pre>
     */
    public void validateCVBatch(com.mvp.videoprocessing.grpc.CVValidationRequest request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.CVValidationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getValidateCVBatchMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void classifyKnowledgeBatch(com.mvp.videoprocessing.grpc.KnowledgeClassificationRequest request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.KnowledgeClassificationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getClassifyKnowledgeBatchMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void generateMaterialRequests(com.mvp.videoprocessing.grpc.GenerateMaterialRequestsRequest request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.GenerateMaterialRequestsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGenerateMaterialRequestsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * 🚀 V6: 资源释放
     * </pre>
     */
    public void releaseCVResources(com.mvp.videoprocessing.grpc.ReleaseResourcesRequest request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.ReleaseResourcesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReleaseCVResourcesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * 健康检查
     * </pre>
     */
    public void healthCheck(com.mvp.videoprocessing.grpc.HealthCheckRequest request,
        io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.HealthCheckResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getHealthCheckMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service VideoProcessingService.
   */
  public static final class VideoProcessingServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<VideoProcessingServiceBlockingStub> {
    private VideoProcessingServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected VideoProcessingServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new VideoProcessingServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * 步骤1: 下载视频 (IO密集 - Python)
     * </pre>
     */
    public com.mvp.videoprocessing.grpc.DownloadResponse downloadVideo(com.mvp.videoprocessing.grpc.DownloadRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDownloadVideoMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * 步骤2: Whisper转录 (GPU/CPU密集 - Python)
     * </pre>
     */
    public com.mvp.videoprocessing.grpc.TranscribeResponse transcribeVideo(com.mvp.videoprocessing.grpc.TranscribeRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getTranscribeVideoMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * 步骤3: Stage1处理(步骤1-6) (LLM API调用 - Python)
     * </pre>
     */
    public com.mvp.videoprocessing.grpc.Stage1Response processStage1(com.mvp.videoprocessing.grpc.Stage1Request request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getProcessStage1Method(), getCallOptions(), request);
    }

    /**
     * <pre>
     * 🔑 V2: Module2 拆分为两阶段
     * 步骤4: Module2 Phase2A - 语义分析 + 时间戳提取 (不执行FFmpeg)
     * </pre>
     */
    public com.mvp.videoprocessing.grpc.AnalyzeResponse analyzeSemanticUnits(com.mvp.videoprocessing.grpc.AnalyzeRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAnalyzeSemanticUnitsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * 步骤6: Module2 Phase2B - Vision AI验证 + 富文本组装 (使用外部截图)
     * </pre>
     */
    public com.mvp.videoprocessing.grpc.AssembleResponse assembleRichText(com.mvp.videoprocessing.grpc.AssembleRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAssembleRichTextMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * 🚀 V3: CV验证批量并行处理 (Java编排调用 - 流式返回)
     * </pre>
     */
    public java.util.Iterator<com.mvp.videoprocessing.grpc.CVValidationResponse> validateCVBatch(
        com.mvp.videoprocessing.grpc.CVValidationRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getValidateCVBatchMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.mvp.videoprocessing.grpc.KnowledgeClassificationResponse classifyKnowledgeBatch(com.mvp.videoprocessing.grpc.KnowledgeClassificationRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getClassifyKnowledgeBatchMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.mvp.videoprocessing.grpc.GenerateMaterialRequestsResponse generateMaterialRequests(com.mvp.videoprocessing.grpc.GenerateMaterialRequestsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGenerateMaterialRequestsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * 🚀 V6: 资源释放
     * </pre>
     */
    public com.mvp.videoprocessing.grpc.ReleaseResourcesResponse releaseCVResources(com.mvp.videoprocessing.grpc.ReleaseResourcesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReleaseCVResourcesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * 健康检查
     * </pre>
     */
    public com.mvp.videoprocessing.grpc.HealthCheckResponse healthCheck(com.mvp.videoprocessing.grpc.HealthCheckRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getHealthCheckMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service VideoProcessingService.
   */
  public static final class VideoProcessingServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<VideoProcessingServiceFutureStub> {
    private VideoProcessingServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected VideoProcessingServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new VideoProcessingServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * 步骤1: 下载视频 (IO密集 - Python)
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.videoprocessing.grpc.DownloadResponse> downloadVideo(
        com.mvp.videoprocessing.grpc.DownloadRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDownloadVideoMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * 步骤2: Whisper转录 (GPU/CPU密集 - Python)
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.videoprocessing.grpc.TranscribeResponse> transcribeVideo(
        com.mvp.videoprocessing.grpc.TranscribeRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getTranscribeVideoMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * 步骤3: Stage1处理(步骤1-6) (LLM API调用 - Python)
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.videoprocessing.grpc.Stage1Response> processStage1(
        com.mvp.videoprocessing.grpc.Stage1Request request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getProcessStage1Method(), getCallOptions()), request);
    }

    /**
     * <pre>
     * 🔑 V2: Module2 拆分为两阶段
     * 步骤4: Module2 Phase2A - 语义分析 + 时间戳提取 (不执行FFmpeg)
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.videoprocessing.grpc.AnalyzeResponse> analyzeSemanticUnits(
        com.mvp.videoprocessing.grpc.AnalyzeRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAnalyzeSemanticUnitsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * 步骤6: Module2 Phase2B - Vision AI验证 + 富文本组装 (使用外部截图)
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.videoprocessing.grpc.AssembleResponse> assembleRichText(
        com.mvp.videoprocessing.grpc.AssembleRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAssembleRichTextMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.videoprocessing.grpc.KnowledgeClassificationResponse> classifyKnowledgeBatch(
        com.mvp.videoprocessing.grpc.KnowledgeClassificationRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getClassifyKnowledgeBatchMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.videoprocessing.grpc.GenerateMaterialRequestsResponse> generateMaterialRequests(
        com.mvp.videoprocessing.grpc.GenerateMaterialRequestsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGenerateMaterialRequestsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * 🚀 V6: 资源释放
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.videoprocessing.grpc.ReleaseResourcesResponse> releaseCVResources(
        com.mvp.videoprocessing.grpc.ReleaseResourcesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReleaseCVResourcesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * 健康检查
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.videoprocessing.grpc.HealthCheckResponse> healthCheck(
        com.mvp.videoprocessing.grpc.HealthCheckRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getHealthCheckMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_DOWNLOAD_VIDEO = 0;
  private static final int METHODID_TRANSCRIBE_VIDEO = 1;
  private static final int METHODID_PROCESS_STAGE1 = 2;
  private static final int METHODID_ANALYZE_SEMANTIC_UNITS = 3;
  private static final int METHODID_ASSEMBLE_RICH_TEXT = 4;
  private static final int METHODID_VALIDATE_CVBATCH = 5;
  private static final int METHODID_CLASSIFY_KNOWLEDGE_BATCH = 6;
  private static final int METHODID_GENERATE_MATERIAL_REQUESTS = 7;
  private static final int METHODID_RELEASE_CVRESOURCES = 8;
  private static final int METHODID_HEALTH_CHECK = 9;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_DOWNLOAD_VIDEO:
          serviceImpl.downloadVideo((com.mvp.videoprocessing.grpc.DownloadRequest) request,
              (io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.DownloadResponse>) responseObserver);
          break;
        case METHODID_TRANSCRIBE_VIDEO:
          serviceImpl.transcribeVideo((com.mvp.videoprocessing.grpc.TranscribeRequest) request,
              (io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.TranscribeResponse>) responseObserver);
          break;
        case METHODID_PROCESS_STAGE1:
          serviceImpl.processStage1((com.mvp.videoprocessing.grpc.Stage1Request) request,
              (io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.Stage1Response>) responseObserver);
          break;
        case METHODID_ANALYZE_SEMANTIC_UNITS:
          serviceImpl.analyzeSemanticUnits((com.mvp.videoprocessing.grpc.AnalyzeRequest) request,
              (io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.AnalyzeResponse>) responseObserver);
          break;
        case METHODID_ASSEMBLE_RICH_TEXT:
          serviceImpl.assembleRichText((com.mvp.videoprocessing.grpc.AssembleRequest) request,
              (io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.AssembleResponse>) responseObserver);
          break;
        case METHODID_VALIDATE_CVBATCH:
          serviceImpl.validateCVBatch((com.mvp.videoprocessing.grpc.CVValidationRequest) request,
              (io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.CVValidationResponse>) responseObserver);
          break;
        case METHODID_CLASSIFY_KNOWLEDGE_BATCH:
          serviceImpl.classifyKnowledgeBatch((com.mvp.videoprocessing.grpc.KnowledgeClassificationRequest) request,
              (io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.KnowledgeClassificationResponse>) responseObserver);
          break;
        case METHODID_GENERATE_MATERIAL_REQUESTS:
          serviceImpl.generateMaterialRequests((com.mvp.videoprocessing.grpc.GenerateMaterialRequestsRequest) request,
              (io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.GenerateMaterialRequestsResponse>) responseObserver);
          break;
        case METHODID_RELEASE_CVRESOURCES:
          serviceImpl.releaseCVResources((com.mvp.videoprocessing.grpc.ReleaseResourcesRequest) request,
              (io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.ReleaseResourcesResponse>) responseObserver);
          break;
        case METHODID_HEALTH_CHECK:
          serviceImpl.healthCheck((com.mvp.videoprocessing.grpc.HealthCheckRequest) request,
              (io.grpc.stub.StreamObserver<com.mvp.videoprocessing.grpc.HealthCheckResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getDownloadVideoMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.videoprocessing.grpc.DownloadRequest,
              com.mvp.videoprocessing.grpc.DownloadResponse>(
                service, METHODID_DOWNLOAD_VIDEO)))
        .addMethod(
          getTranscribeVideoMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.videoprocessing.grpc.TranscribeRequest,
              com.mvp.videoprocessing.grpc.TranscribeResponse>(
                service, METHODID_TRANSCRIBE_VIDEO)))
        .addMethod(
          getProcessStage1Method(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.videoprocessing.grpc.Stage1Request,
              com.mvp.videoprocessing.grpc.Stage1Response>(
                service, METHODID_PROCESS_STAGE1)))
        .addMethod(
          getAnalyzeSemanticUnitsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.videoprocessing.grpc.AnalyzeRequest,
              com.mvp.videoprocessing.grpc.AnalyzeResponse>(
                service, METHODID_ANALYZE_SEMANTIC_UNITS)))
        .addMethod(
          getAssembleRichTextMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.videoprocessing.grpc.AssembleRequest,
              com.mvp.videoprocessing.grpc.AssembleResponse>(
                service, METHODID_ASSEMBLE_RICH_TEXT)))
        .addMethod(
          getValidateCVBatchMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              com.mvp.videoprocessing.grpc.CVValidationRequest,
              com.mvp.videoprocessing.grpc.CVValidationResponse>(
                service, METHODID_VALIDATE_CVBATCH)))
        .addMethod(
          getClassifyKnowledgeBatchMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.videoprocessing.grpc.KnowledgeClassificationRequest,
              com.mvp.videoprocessing.grpc.KnowledgeClassificationResponse>(
                service, METHODID_CLASSIFY_KNOWLEDGE_BATCH)))
        .addMethod(
          getGenerateMaterialRequestsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.videoprocessing.grpc.GenerateMaterialRequestsRequest,
              com.mvp.videoprocessing.grpc.GenerateMaterialRequestsResponse>(
                service, METHODID_GENERATE_MATERIAL_REQUESTS)))
        .addMethod(
          getReleaseCVResourcesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.videoprocessing.grpc.ReleaseResourcesRequest,
              com.mvp.videoprocessing.grpc.ReleaseResourcesResponse>(
                service, METHODID_RELEASE_CVRESOURCES)))
        .addMethod(
          getHealthCheckMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.videoprocessing.grpc.HealthCheckRequest,
              com.mvp.videoprocessing.grpc.HealthCheckResponse>(
                service, METHODID_HEALTH_CHECK)))
        .build();
  }

  private static abstract class VideoProcessingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    VideoProcessingServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.mvp.videoprocessing.grpc.VideoProcessingProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("VideoProcessingService");
    }
  }

  private static final class VideoProcessingServiceFileDescriptorSupplier
      extends VideoProcessingServiceBaseDescriptorSupplier {
    VideoProcessingServiceFileDescriptorSupplier() {}
  }

  private static final class VideoProcessingServiceMethodDescriptorSupplier
      extends VideoProcessingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    VideoProcessingServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (VideoProcessingServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new VideoProcessingServiceFileDescriptorSupplier())
              .addMethod(getDownloadVideoMethod())
              .addMethod(getTranscribeVideoMethod())
              .addMethod(getProcessStage1Method())
              .addMethod(getAnalyzeSemanticUnitsMethod())
              .addMethod(getAssembleRichTextMethod())
              .addMethod(getValidateCVBatchMethod())
              .addMethod(getClassifyKnowledgeBatchMethod())
              .addMethod(getGenerateMaterialRequestsMethod())
              .addMethod(getReleaseCVResourcesMethod())
              .addMethod(getHealthCheckMethod())
              .build();
        }
      }
    }
    return result;
  }
}
