package com.mvp.module2.fusion.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.63.0)",
    comments = "Source: fusion_service.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class FusionComputeServiceGrpc {

  private FusionComputeServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "com.mvp.module2.fusion.grpc.FusionComputeService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.FeatureRequest,
      com.mvp.module2.fusion.grpc.FeatureResponse> getExtractFeaturesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ExtractFeatures",
      requestType = com.mvp.module2.fusion.grpc.FeatureRequest.class,
      responseType = com.mvp.module2.fusion.grpc.FeatureResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.FeatureRequest,
      com.mvp.module2.fusion.grpc.FeatureResponse> getExtractFeaturesMethod() {
    io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.FeatureRequest, com.mvp.module2.fusion.grpc.FeatureResponse> getExtractFeaturesMethod;
    if ((getExtractFeaturesMethod = FusionComputeServiceGrpc.getExtractFeaturesMethod) == null) {
      synchronized (FusionComputeServiceGrpc.class) {
        if ((getExtractFeaturesMethod = FusionComputeServiceGrpc.getExtractFeaturesMethod) == null) {
          FusionComputeServiceGrpc.getExtractFeaturesMethod = getExtractFeaturesMethod =
              io.grpc.MethodDescriptor.<com.mvp.module2.fusion.grpc.FeatureRequest, com.mvp.module2.fusion.grpc.FeatureResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ExtractFeatures"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.FeatureRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.FeatureResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FusionComputeServiceMethodDescriptorSupplier("ExtractFeatures"))
              .build();
        }
      }
    }
    return getExtractFeaturesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.BatchFeatureRequest,
      com.mvp.module2.fusion.grpc.BatchFeatureResponse> getExtractFeaturesBatchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ExtractFeaturesBatch",
      requestType = com.mvp.module2.fusion.grpc.BatchFeatureRequest.class,
      responseType = com.mvp.module2.fusion.grpc.BatchFeatureResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.BatchFeatureRequest,
      com.mvp.module2.fusion.grpc.BatchFeatureResponse> getExtractFeaturesBatchMethod() {
    io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.BatchFeatureRequest, com.mvp.module2.fusion.grpc.BatchFeatureResponse> getExtractFeaturesBatchMethod;
    if ((getExtractFeaturesBatchMethod = FusionComputeServiceGrpc.getExtractFeaturesBatchMethod) == null) {
      synchronized (FusionComputeServiceGrpc.class) {
        if ((getExtractFeaturesBatchMethod = FusionComputeServiceGrpc.getExtractFeaturesBatchMethod) == null) {
          FusionComputeServiceGrpc.getExtractFeaturesBatchMethod = getExtractFeaturesBatchMethod =
              io.grpc.MethodDescriptor.<com.mvp.module2.fusion.grpc.BatchFeatureRequest, com.mvp.module2.fusion.grpc.BatchFeatureResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ExtractFeaturesBatch"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.BatchFeatureRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.BatchFeatureResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FusionComputeServiceMethodDescriptorSupplier("ExtractFeaturesBatch"))
              .build();
        }
      }
    }
    return getExtractFeaturesBatchMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.Empty,
      com.mvp.module2.fusion.grpc.HealthStatus> getPingMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Ping",
      requestType = com.mvp.module2.fusion.grpc.Empty.class,
      responseType = com.mvp.module2.fusion.grpc.HealthStatus.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.Empty,
      com.mvp.module2.fusion.grpc.HealthStatus> getPingMethod() {
    io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.Empty, com.mvp.module2.fusion.grpc.HealthStatus> getPingMethod;
    if ((getPingMethod = FusionComputeServiceGrpc.getPingMethod) == null) {
      synchronized (FusionComputeServiceGrpc.class) {
        if ((getPingMethod = FusionComputeServiceGrpc.getPingMethod) == null) {
          FusionComputeServiceGrpc.getPingMethod = getPingMethod =
              io.grpc.MethodDescriptor.<com.mvp.module2.fusion.grpc.Empty, com.mvp.module2.fusion.grpc.HealthStatus>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Ping"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.Empty.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.HealthStatus.getDefaultInstance()))
              .setSchemaDescriptor(new FusionComputeServiceMethodDescriptorSupplier("Ping"))
              .build();
        }
      }
    }
    return getPingMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.FrameSelectionRequest,
      com.mvp.module2.fusion.grpc.FrameSelectionResponse> getSelectBestFrameMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SelectBestFrame",
      requestType = com.mvp.module2.fusion.grpc.FrameSelectionRequest.class,
      responseType = com.mvp.module2.fusion.grpc.FrameSelectionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.FrameSelectionRequest,
      com.mvp.module2.fusion.grpc.FrameSelectionResponse> getSelectBestFrameMethod() {
    io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.FrameSelectionRequest, com.mvp.module2.fusion.grpc.FrameSelectionResponse> getSelectBestFrameMethod;
    if ((getSelectBestFrameMethod = FusionComputeServiceGrpc.getSelectBestFrameMethod) == null) {
      synchronized (FusionComputeServiceGrpc.class) {
        if ((getSelectBestFrameMethod = FusionComputeServiceGrpc.getSelectBestFrameMethod) == null) {
          FusionComputeServiceGrpc.getSelectBestFrameMethod = getSelectBestFrameMethod =
              io.grpc.MethodDescriptor.<com.mvp.module2.fusion.grpc.FrameSelectionRequest, com.mvp.module2.fusion.grpc.FrameSelectionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SelectBestFrame"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.FrameSelectionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.FrameSelectionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FusionComputeServiceMethodDescriptorSupplier("SelectBestFrame"))
              .build();
        }
      }
    }
    return getSelectBestFrameMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.FrameHashRequest,
      com.mvp.module2.fusion.grpc.FrameHashResponse> getComputeFrameHashMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ComputeFrameHash",
      requestType = com.mvp.module2.fusion.grpc.FrameHashRequest.class,
      responseType = com.mvp.module2.fusion.grpc.FrameHashResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.FrameHashRequest,
      com.mvp.module2.fusion.grpc.FrameHashResponse> getComputeFrameHashMethod() {
    io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.FrameHashRequest, com.mvp.module2.fusion.grpc.FrameHashResponse> getComputeFrameHashMethod;
    if ((getComputeFrameHashMethod = FusionComputeServiceGrpc.getComputeFrameHashMethod) == null) {
      synchronized (FusionComputeServiceGrpc.class) {
        if ((getComputeFrameHashMethod = FusionComputeServiceGrpc.getComputeFrameHashMethod) == null) {
          FusionComputeServiceGrpc.getComputeFrameHashMethod = getComputeFrameHashMethod =
              io.grpc.MethodDescriptor.<com.mvp.module2.fusion.grpc.FrameHashRequest, com.mvp.module2.fusion.grpc.FrameHashResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ComputeFrameHash"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.FrameHashRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.FrameHashResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FusionComputeServiceMethodDescriptorSupplier("ComputeFrameHash"))
              .build();
        }
      }
    }
    return getComputeFrameHashMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.DetectFaultsRequest,
      com.mvp.module2.fusion.grpc.DetectFaultsResponse> getDetectFaultsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DetectFaults",
      requestType = com.mvp.module2.fusion.grpc.DetectFaultsRequest.class,
      responseType = com.mvp.module2.fusion.grpc.DetectFaultsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.DetectFaultsRequest,
      com.mvp.module2.fusion.grpc.DetectFaultsResponse> getDetectFaultsMethod() {
    io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.DetectFaultsRequest, com.mvp.module2.fusion.grpc.DetectFaultsResponse> getDetectFaultsMethod;
    if ((getDetectFaultsMethod = FusionComputeServiceGrpc.getDetectFaultsMethod) == null) {
      synchronized (FusionComputeServiceGrpc.class) {
        if ((getDetectFaultsMethod = FusionComputeServiceGrpc.getDetectFaultsMethod) == null) {
          FusionComputeServiceGrpc.getDetectFaultsMethod = getDetectFaultsMethod =
              io.grpc.MethodDescriptor.<com.mvp.module2.fusion.grpc.DetectFaultsRequest, com.mvp.module2.fusion.grpc.DetectFaultsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DetectFaults"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.DetectFaultsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.DetectFaultsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FusionComputeServiceMethodDescriptorSupplier("DetectFaults"))
              .build();
        }
      }
    }
    return getDetectFaultsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.GenerateTextRequest,
      com.mvp.module2.fusion.grpc.GenerateTextResponse> getGenerateEnhancementTextMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GenerateEnhancementText",
      requestType = com.mvp.module2.fusion.grpc.GenerateTextRequest.class,
      responseType = com.mvp.module2.fusion.grpc.GenerateTextResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.GenerateTextRequest,
      com.mvp.module2.fusion.grpc.GenerateTextResponse> getGenerateEnhancementTextMethod() {
    io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.GenerateTextRequest, com.mvp.module2.fusion.grpc.GenerateTextResponse> getGenerateEnhancementTextMethod;
    if ((getGenerateEnhancementTextMethod = FusionComputeServiceGrpc.getGenerateEnhancementTextMethod) == null) {
      synchronized (FusionComputeServiceGrpc.class) {
        if ((getGenerateEnhancementTextMethod = FusionComputeServiceGrpc.getGenerateEnhancementTextMethod) == null) {
          FusionComputeServiceGrpc.getGenerateEnhancementTextMethod = getGenerateEnhancementTextMethod =
              io.grpc.MethodDescriptor.<com.mvp.module2.fusion.grpc.GenerateTextRequest, com.mvp.module2.fusion.grpc.GenerateTextResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GenerateEnhancementText"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.GenerateTextRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.GenerateTextResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FusionComputeServiceMethodDescriptorSupplier("GenerateEnhancementText"))
              .build();
        }
      }
    }
    return getGenerateEnhancementTextMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.OptimizeMaterialsRequest,
      com.mvp.module2.fusion.grpc.OptimizeMaterialsResponse> getOptimizeMaterialsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "OptimizeMaterials",
      requestType = com.mvp.module2.fusion.grpc.OptimizeMaterialsRequest.class,
      responseType = com.mvp.module2.fusion.grpc.OptimizeMaterialsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.OptimizeMaterialsRequest,
      com.mvp.module2.fusion.grpc.OptimizeMaterialsResponse> getOptimizeMaterialsMethod() {
    io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.OptimizeMaterialsRequest, com.mvp.module2.fusion.grpc.OptimizeMaterialsResponse> getOptimizeMaterialsMethod;
    if ((getOptimizeMaterialsMethod = FusionComputeServiceGrpc.getOptimizeMaterialsMethod) == null) {
      synchronized (FusionComputeServiceGrpc.class) {
        if ((getOptimizeMaterialsMethod = FusionComputeServiceGrpc.getOptimizeMaterialsMethod) == null) {
          FusionComputeServiceGrpc.getOptimizeMaterialsMethod = getOptimizeMaterialsMethod =
              io.grpc.MethodDescriptor.<com.mvp.module2.fusion.grpc.OptimizeMaterialsRequest, com.mvp.module2.fusion.grpc.OptimizeMaterialsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "OptimizeMaterials"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.OptimizeMaterialsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.OptimizeMaterialsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FusionComputeServiceMethodDescriptorSupplier("OptimizeMaterials"))
              .build();
        }
      }
    }
    return getOptimizeMaterialsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.VideoClipRequest,
      com.mvp.module2.fusion.grpc.VideoClipResponse> getExtractVideoClipMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ExtractVideoClip",
      requestType = com.mvp.module2.fusion.grpc.VideoClipRequest.class,
      responseType = com.mvp.module2.fusion.grpc.VideoClipResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.VideoClipRequest,
      com.mvp.module2.fusion.grpc.VideoClipResponse> getExtractVideoClipMethod() {
    io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.VideoClipRequest, com.mvp.module2.fusion.grpc.VideoClipResponse> getExtractVideoClipMethod;
    if ((getExtractVideoClipMethod = FusionComputeServiceGrpc.getExtractVideoClipMethod) == null) {
      synchronized (FusionComputeServiceGrpc.class) {
        if ((getExtractVideoClipMethod = FusionComputeServiceGrpc.getExtractVideoClipMethod) == null) {
          FusionComputeServiceGrpc.getExtractVideoClipMethod = getExtractVideoClipMethod =
              io.grpc.MethodDescriptor.<com.mvp.module2.fusion.grpc.VideoClipRequest, com.mvp.module2.fusion.grpc.VideoClipResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ExtractVideoClip"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.VideoClipRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.VideoClipResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FusionComputeServiceMethodDescriptorSupplier("ExtractVideoClip"))
              .build();
        }
      }
    }
    return getExtractVideoClipMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.ModalityClassificationRequest,
      com.mvp.module2.fusion.grpc.ModalityClassificationResponse> getGetModalityClassificationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetModalityClassification",
      requestType = com.mvp.module2.fusion.grpc.ModalityClassificationRequest.class,
      responseType = com.mvp.module2.fusion.grpc.ModalityClassificationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.ModalityClassificationRequest,
      com.mvp.module2.fusion.grpc.ModalityClassificationResponse> getGetModalityClassificationMethod() {
    io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.ModalityClassificationRequest, com.mvp.module2.fusion.grpc.ModalityClassificationResponse> getGetModalityClassificationMethod;
    if ((getGetModalityClassificationMethod = FusionComputeServiceGrpc.getGetModalityClassificationMethod) == null) {
      synchronized (FusionComputeServiceGrpc.class) {
        if ((getGetModalityClassificationMethod = FusionComputeServiceGrpc.getGetModalityClassificationMethod) == null) {
          FusionComputeServiceGrpc.getGetModalityClassificationMethod = getGetModalityClassificationMethod =
              io.grpc.MethodDescriptor.<com.mvp.module2.fusion.grpc.ModalityClassificationRequest, com.mvp.module2.fusion.grpc.ModalityClassificationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetModalityClassification"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.ModalityClassificationRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.ModalityClassificationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FusionComputeServiceMethodDescriptorSupplier("GetModalityClassification"))
              .build();
        }
      }
    }
    return getGetModalityClassificationMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.GenerateScreenshotRequest,
      com.mvp.module2.fusion.grpc.GenerateScreenshotResponse> getGenerateScreenshotMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GenerateScreenshot",
      requestType = com.mvp.module2.fusion.grpc.GenerateScreenshotRequest.class,
      responseType = com.mvp.module2.fusion.grpc.GenerateScreenshotResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.GenerateScreenshotRequest,
      com.mvp.module2.fusion.grpc.GenerateScreenshotResponse> getGenerateScreenshotMethod() {
    io.grpc.MethodDescriptor<com.mvp.module2.fusion.grpc.GenerateScreenshotRequest, com.mvp.module2.fusion.grpc.GenerateScreenshotResponse> getGenerateScreenshotMethod;
    if ((getGenerateScreenshotMethod = FusionComputeServiceGrpc.getGenerateScreenshotMethod) == null) {
      synchronized (FusionComputeServiceGrpc.class) {
        if ((getGenerateScreenshotMethod = FusionComputeServiceGrpc.getGenerateScreenshotMethod) == null) {
          FusionComputeServiceGrpc.getGenerateScreenshotMethod = getGenerateScreenshotMethod =
              io.grpc.MethodDescriptor.<com.mvp.module2.fusion.grpc.GenerateScreenshotRequest, com.mvp.module2.fusion.grpc.GenerateScreenshotResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GenerateScreenshot"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.GenerateScreenshotRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.mvp.module2.fusion.grpc.GenerateScreenshotResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FusionComputeServiceMethodDescriptorSupplier("GenerateScreenshot"))
              .build();
        }
      }
    }
    return getGenerateScreenshotMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static FusionComputeServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<FusionComputeServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<FusionComputeServiceStub>() {
        @java.lang.Override
        public FusionComputeServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new FusionComputeServiceStub(channel, callOptions);
        }
      };
    return FusionComputeServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static FusionComputeServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<FusionComputeServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<FusionComputeServiceBlockingStub>() {
        @java.lang.Override
        public FusionComputeServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new FusionComputeServiceBlockingStub(channel, callOptions);
        }
      };
    return FusionComputeServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static FusionComputeServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<FusionComputeServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<FusionComputeServiceFutureStub>() {
        @java.lang.Override
        public FusionComputeServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new FusionComputeServiceFutureStub(channel, callOptions);
        }
      };
    return FusionComputeServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     * <pre>
     * 1. Submit a video for feature extraction (Sync/Blocking)
     * Java calls this to trigger Python processing.
     * </pre>
     */
    default void extractFeatures(com.mvp.module2.fusion.grpc.FeatureRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.FeatureResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExtractFeaturesMethod(), responseObserver);
    }

    /**
     * <pre>
     * 1.1 BATCH: Submit multiple segments at once to avoid connection overhead
     * </pre>
     */
    default void extractFeaturesBatch(com.mvp.module2.fusion.grpc.BatchFeatureRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.BatchFeatureResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExtractFeaturesBatchMethod(), responseObserver);
    }

    /**
     * <pre>
     * 2. Health Check (for Java Sentinel/Resilience4j)
     * </pre>
     */
    default void ping(com.mvp.module2.fusion.grpc.Empty request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.HealthStatus> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPingMethod(), responseObserver);
    }

    /**
     * <pre>
     * 3. Select the best screenshot from a video segment
     * </pre>
     */
    default void selectBestFrame(com.mvp.module2.fusion.grpc.FrameSelectionRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.FrameSelectionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSelectBestFrameMethod(), responseObserver);
    }

    /**
     * <pre>
     * 4. Compute perceptual hash for a list of frames (for MaterialOptimizer)
     * </pre>
     */
    default void computeFrameHash(com.mvp.module2.fusion.grpc.FrameHashRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.FrameHashResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getComputeFrameHashMethod(), responseObserver);
    }

    /**
     * <pre>
     * 5. Strict Parity: Fault Detection utilizing Python LLM
     * </pre>
     */
    default void detectFaults(com.mvp.module2.fusion.grpc.DetectFaultsRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.DetectFaultsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDetectFaultsMethod(), responseObserver);
    }

    /**
     * <pre>
     * 6. Strict Parity: Cognitive Text Generation
     * </pre>
     */
    default void generateEnhancementText(com.mvp.module2.fusion.grpc.GenerateTextRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.GenerateTextResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGenerateEnhancementTextMethod(), responseObserver);
    }

    /**
     * <pre>
     * 7. Strict Parity: Global Material Optimization (Clustering &amp; Deduplication)
     * </pre>
     */
    default void optimizeMaterials(com.mvp.module2.fusion.grpc.OptimizeMaterialsRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.OptimizeMaterialsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getOptimizeMaterialsMethod(), responseObserver);
    }

    /**
     * <pre>
     * 8. Extract a video clip from the source video
     * </pre>
     */
    default void extractVideoClip(com.mvp.module2.fusion.grpc.VideoClipRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.VideoClipResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExtractVideoClipMethod(), responseObserver);
    }

    /**
     * <pre>
     * ========== V7.x: Modality Classification ==========
     * 9. Get modality classification for a video segment
     * Python performs CV analysis and returns modality decision
     * </pre>
     */
    default void getModalityClassification(com.mvp.module2.fusion.grpc.ModalityClassificationRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.ModalityClassificationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetModalityClassificationMethod(), responseObserver);
    }

    /**
     * <pre>
     * 10. Generate screenshot at specific timestamp
     * </pre>
     */
    default void generateScreenshot(com.mvp.module2.fusion.grpc.GenerateScreenshotRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.GenerateScreenshotResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGenerateScreenshotMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service FusionComputeService.
   */
  public static abstract class FusionComputeServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return FusionComputeServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service FusionComputeService.
   */
  public static final class FusionComputeServiceStub
      extends io.grpc.stub.AbstractAsyncStub<FusionComputeServiceStub> {
    private FusionComputeServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FusionComputeServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new FusionComputeServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     * 1. Submit a video for feature extraction (Sync/Blocking)
     * Java calls this to trigger Python processing.
     * </pre>
     */
    public void extractFeatures(com.mvp.module2.fusion.grpc.FeatureRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.FeatureResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getExtractFeaturesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * 1.1 BATCH: Submit multiple segments at once to avoid connection overhead
     * </pre>
     */
    public void extractFeaturesBatch(com.mvp.module2.fusion.grpc.BatchFeatureRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.BatchFeatureResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getExtractFeaturesBatchMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * 2. Health Check (for Java Sentinel/Resilience4j)
     * </pre>
     */
    public void ping(com.mvp.module2.fusion.grpc.Empty request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.HealthStatus> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPingMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * 3. Select the best screenshot from a video segment
     * </pre>
     */
    public void selectBestFrame(com.mvp.module2.fusion.grpc.FrameSelectionRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.FrameSelectionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSelectBestFrameMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * 4. Compute perceptual hash for a list of frames (for MaterialOptimizer)
     * </pre>
     */
    public void computeFrameHash(com.mvp.module2.fusion.grpc.FrameHashRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.FrameHashResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getComputeFrameHashMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * 5. Strict Parity: Fault Detection utilizing Python LLM
     * </pre>
     */
    public void detectFaults(com.mvp.module2.fusion.grpc.DetectFaultsRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.DetectFaultsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDetectFaultsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * 6. Strict Parity: Cognitive Text Generation
     * </pre>
     */
    public void generateEnhancementText(com.mvp.module2.fusion.grpc.GenerateTextRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.GenerateTextResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGenerateEnhancementTextMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * 7. Strict Parity: Global Material Optimization (Clustering &amp; Deduplication)
     * </pre>
     */
    public void optimizeMaterials(com.mvp.module2.fusion.grpc.OptimizeMaterialsRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.OptimizeMaterialsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getOptimizeMaterialsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * 8. Extract a video clip from the source video
     * </pre>
     */
    public void extractVideoClip(com.mvp.module2.fusion.grpc.VideoClipRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.VideoClipResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getExtractVideoClipMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * ========== V7.x: Modality Classification ==========
     * 9. Get modality classification for a video segment
     * Python performs CV analysis and returns modality decision
     * </pre>
     */
    public void getModalityClassification(com.mvp.module2.fusion.grpc.ModalityClassificationRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.ModalityClassificationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetModalityClassificationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * 10. Generate screenshot at specific timestamp
     * </pre>
     */
    public void generateScreenshot(com.mvp.module2.fusion.grpc.GenerateScreenshotRequest request,
        io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.GenerateScreenshotResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGenerateScreenshotMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service FusionComputeService.
   */
  public static final class FusionComputeServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<FusionComputeServiceBlockingStub> {
    private FusionComputeServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FusionComputeServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new FusionComputeServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * 1. Submit a video for feature extraction (Sync/Blocking)
     * Java calls this to trigger Python processing.
     * </pre>
     */
    public com.mvp.module2.fusion.grpc.FeatureResponse extractFeatures(com.mvp.module2.fusion.grpc.FeatureRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getExtractFeaturesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * 1.1 BATCH: Submit multiple segments at once to avoid connection overhead
     * </pre>
     */
    public com.mvp.module2.fusion.grpc.BatchFeatureResponse extractFeaturesBatch(com.mvp.module2.fusion.grpc.BatchFeatureRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getExtractFeaturesBatchMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * 2. Health Check (for Java Sentinel/Resilience4j)
     * </pre>
     */
    public com.mvp.module2.fusion.grpc.HealthStatus ping(com.mvp.module2.fusion.grpc.Empty request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPingMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * 3. Select the best screenshot from a video segment
     * </pre>
     */
    public com.mvp.module2.fusion.grpc.FrameSelectionResponse selectBestFrame(com.mvp.module2.fusion.grpc.FrameSelectionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSelectBestFrameMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * 4. Compute perceptual hash for a list of frames (for MaterialOptimizer)
     * </pre>
     */
    public com.mvp.module2.fusion.grpc.FrameHashResponse computeFrameHash(com.mvp.module2.fusion.grpc.FrameHashRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getComputeFrameHashMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * 5. Strict Parity: Fault Detection utilizing Python LLM
     * </pre>
     */
    public com.mvp.module2.fusion.grpc.DetectFaultsResponse detectFaults(com.mvp.module2.fusion.grpc.DetectFaultsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDetectFaultsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * 6. Strict Parity: Cognitive Text Generation
     * </pre>
     */
    public com.mvp.module2.fusion.grpc.GenerateTextResponse generateEnhancementText(com.mvp.module2.fusion.grpc.GenerateTextRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGenerateEnhancementTextMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * 7. Strict Parity: Global Material Optimization (Clustering &amp; Deduplication)
     * </pre>
     */
    public com.mvp.module2.fusion.grpc.OptimizeMaterialsResponse optimizeMaterials(com.mvp.module2.fusion.grpc.OptimizeMaterialsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getOptimizeMaterialsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * 8. Extract a video clip from the source video
     * </pre>
     */
    public com.mvp.module2.fusion.grpc.VideoClipResponse extractVideoClip(com.mvp.module2.fusion.grpc.VideoClipRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getExtractVideoClipMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * ========== V7.x: Modality Classification ==========
     * 9. Get modality classification for a video segment
     * Python performs CV analysis and returns modality decision
     * </pre>
     */
    public com.mvp.module2.fusion.grpc.ModalityClassificationResponse getModalityClassification(com.mvp.module2.fusion.grpc.ModalityClassificationRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetModalityClassificationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * 10. Generate screenshot at specific timestamp
     * </pre>
     */
    public com.mvp.module2.fusion.grpc.GenerateScreenshotResponse generateScreenshot(com.mvp.module2.fusion.grpc.GenerateScreenshotRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGenerateScreenshotMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service FusionComputeService.
   */
  public static final class FusionComputeServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<FusionComputeServiceFutureStub> {
    private FusionComputeServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FusionComputeServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new FusionComputeServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * 1. Submit a video for feature extraction (Sync/Blocking)
     * Java calls this to trigger Python processing.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.module2.fusion.grpc.FeatureResponse> extractFeatures(
        com.mvp.module2.fusion.grpc.FeatureRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getExtractFeaturesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * 1.1 BATCH: Submit multiple segments at once to avoid connection overhead
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.module2.fusion.grpc.BatchFeatureResponse> extractFeaturesBatch(
        com.mvp.module2.fusion.grpc.BatchFeatureRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getExtractFeaturesBatchMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * 2. Health Check (for Java Sentinel/Resilience4j)
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.module2.fusion.grpc.HealthStatus> ping(
        com.mvp.module2.fusion.grpc.Empty request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPingMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * 3. Select the best screenshot from a video segment
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.module2.fusion.grpc.FrameSelectionResponse> selectBestFrame(
        com.mvp.module2.fusion.grpc.FrameSelectionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSelectBestFrameMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * 4. Compute perceptual hash for a list of frames (for MaterialOptimizer)
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.module2.fusion.grpc.FrameHashResponse> computeFrameHash(
        com.mvp.module2.fusion.grpc.FrameHashRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getComputeFrameHashMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * 5. Strict Parity: Fault Detection utilizing Python LLM
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.module2.fusion.grpc.DetectFaultsResponse> detectFaults(
        com.mvp.module2.fusion.grpc.DetectFaultsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDetectFaultsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * 6. Strict Parity: Cognitive Text Generation
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.module2.fusion.grpc.GenerateTextResponse> generateEnhancementText(
        com.mvp.module2.fusion.grpc.GenerateTextRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGenerateEnhancementTextMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * 7. Strict Parity: Global Material Optimization (Clustering &amp; Deduplication)
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.module2.fusion.grpc.OptimizeMaterialsResponse> optimizeMaterials(
        com.mvp.module2.fusion.grpc.OptimizeMaterialsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getOptimizeMaterialsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * 8. Extract a video clip from the source video
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.module2.fusion.grpc.VideoClipResponse> extractVideoClip(
        com.mvp.module2.fusion.grpc.VideoClipRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getExtractVideoClipMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * ========== V7.x: Modality Classification ==========
     * 9. Get modality classification for a video segment
     * Python performs CV analysis and returns modality decision
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.module2.fusion.grpc.ModalityClassificationResponse> getModalityClassification(
        com.mvp.module2.fusion.grpc.ModalityClassificationRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetModalityClassificationMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * 10. Generate screenshot at specific timestamp
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.mvp.module2.fusion.grpc.GenerateScreenshotResponse> generateScreenshot(
        com.mvp.module2.fusion.grpc.GenerateScreenshotRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGenerateScreenshotMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_EXTRACT_FEATURES = 0;
  private static final int METHODID_EXTRACT_FEATURES_BATCH = 1;
  private static final int METHODID_PING = 2;
  private static final int METHODID_SELECT_BEST_FRAME = 3;
  private static final int METHODID_COMPUTE_FRAME_HASH = 4;
  private static final int METHODID_DETECT_FAULTS = 5;
  private static final int METHODID_GENERATE_ENHANCEMENT_TEXT = 6;
  private static final int METHODID_OPTIMIZE_MATERIALS = 7;
  private static final int METHODID_EXTRACT_VIDEO_CLIP = 8;
  private static final int METHODID_GET_MODALITY_CLASSIFICATION = 9;
  private static final int METHODID_GENERATE_SCREENSHOT = 10;

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
        case METHODID_EXTRACT_FEATURES:
          serviceImpl.extractFeatures((com.mvp.module2.fusion.grpc.FeatureRequest) request,
              (io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.FeatureResponse>) responseObserver);
          break;
        case METHODID_EXTRACT_FEATURES_BATCH:
          serviceImpl.extractFeaturesBatch((com.mvp.module2.fusion.grpc.BatchFeatureRequest) request,
              (io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.BatchFeatureResponse>) responseObserver);
          break;
        case METHODID_PING:
          serviceImpl.ping((com.mvp.module2.fusion.grpc.Empty) request,
              (io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.HealthStatus>) responseObserver);
          break;
        case METHODID_SELECT_BEST_FRAME:
          serviceImpl.selectBestFrame((com.mvp.module2.fusion.grpc.FrameSelectionRequest) request,
              (io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.FrameSelectionResponse>) responseObserver);
          break;
        case METHODID_COMPUTE_FRAME_HASH:
          serviceImpl.computeFrameHash((com.mvp.module2.fusion.grpc.FrameHashRequest) request,
              (io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.FrameHashResponse>) responseObserver);
          break;
        case METHODID_DETECT_FAULTS:
          serviceImpl.detectFaults((com.mvp.module2.fusion.grpc.DetectFaultsRequest) request,
              (io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.DetectFaultsResponse>) responseObserver);
          break;
        case METHODID_GENERATE_ENHANCEMENT_TEXT:
          serviceImpl.generateEnhancementText((com.mvp.module2.fusion.grpc.GenerateTextRequest) request,
              (io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.GenerateTextResponse>) responseObserver);
          break;
        case METHODID_OPTIMIZE_MATERIALS:
          serviceImpl.optimizeMaterials((com.mvp.module2.fusion.grpc.OptimizeMaterialsRequest) request,
              (io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.OptimizeMaterialsResponse>) responseObserver);
          break;
        case METHODID_EXTRACT_VIDEO_CLIP:
          serviceImpl.extractVideoClip((com.mvp.module2.fusion.grpc.VideoClipRequest) request,
              (io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.VideoClipResponse>) responseObserver);
          break;
        case METHODID_GET_MODALITY_CLASSIFICATION:
          serviceImpl.getModalityClassification((com.mvp.module2.fusion.grpc.ModalityClassificationRequest) request,
              (io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.ModalityClassificationResponse>) responseObserver);
          break;
        case METHODID_GENERATE_SCREENSHOT:
          serviceImpl.generateScreenshot((com.mvp.module2.fusion.grpc.GenerateScreenshotRequest) request,
              (io.grpc.stub.StreamObserver<com.mvp.module2.fusion.grpc.GenerateScreenshotResponse>) responseObserver);
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
          getExtractFeaturesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.module2.fusion.grpc.FeatureRequest,
              com.mvp.module2.fusion.grpc.FeatureResponse>(
                service, METHODID_EXTRACT_FEATURES)))
        .addMethod(
          getExtractFeaturesBatchMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.module2.fusion.grpc.BatchFeatureRequest,
              com.mvp.module2.fusion.grpc.BatchFeatureResponse>(
                service, METHODID_EXTRACT_FEATURES_BATCH)))
        .addMethod(
          getPingMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.module2.fusion.grpc.Empty,
              com.mvp.module2.fusion.grpc.HealthStatus>(
                service, METHODID_PING)))
        .addMethod(
          getSelectBestFrameMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.module2.fusion.grpc.FrameSelectionRequest,
              com.mvp.module2.fusion.grpc.FrameSelectionResponse>(
                service, METHODID_SELECT_BEST_FRAME)))
        .addMethod(
          getComputeFrameHashMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.module2.fusion.grpc.FrameHashRequest,
              com.mvp.module2.fusion.grpc.FrameHashResponse>(
                service, METHODID_COMPUTE_FRAME_HASH)))
        .addMethod(
          getDetectFaultsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.module2.fusion.grpc.DetectFaultsRequest,
              com.mvp.module2.fusion.grpc.DetectFaultsResponse>(
                service, METHODID_DETECT_FAULTS)))
        .addMethod(
          getGenerateEnhancementTextMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.module2.fusion.grpc.GenerateTextRequest,
              com.mvp.module2.fusion.grpc.GenerateTextResponse>(
                service, METHODID_GENERATE_ENHANCEMENT_TEXT)))
        .addMethod(
          getOptimizeMaterialsMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.module2.fusion.grpc.OptimizeMaterialsRequest,
              com.mvp.module2.fusion.grpc.OptimizeMaterialsResponse>(
                service, METHODID_OPTIMIZE_MATERIALS)))
        .addMethod(
          getExtractVideoClipMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.module2.fusion.grpc.VideoClipRequest,
              com.mvp.module2.fusion.grpc.VideoClipResponse>(
                service, METHODID_EXTRACT_VIDEO_CLIP)))
        .addMethod(
          getGetModalityClassificationMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.module2.fusion.grpc.ModalityClassificationRequest,
              com.mvp.module2.fusion.grpc.ModalityClassificationResponse>(
                service, METHODID_GET_MODALITY_CLASSIFICATION)))
        .addMethod(
          getGenerateScreenshotMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.mvp.module2.fusion.grpc.GenerateScreenshotRequest,
              com.mvp.module2.fusion.grpc.GenerateScreenshotResponse>(
                service, METHODID_GENERATE_SCREENSHOT)))
        .build();
  }

  private static abstract class FusionComputeServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    FusionComputeServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.mvp.module2.fusion.grpc.FusionServiceProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("FusionComputeService");
    }
  }

  private static final class FusionComputeServiceFileDescriptorSupplier
      extends FusionComputeServiceBaseDescriptorSupplier {
    FusionComputeServiceFileDescriptorSupplier() {}
  }

  private static final class FusionComputeServiceMethodDescriptorSupplier
      extends FusionComputeServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    FusionComputeServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (FusionComputeServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new FusionComputeServiceFileDescriptorSupplier())
              .addMethod(getExtractFeaturesMethod())
              .addMethod(getExtractFeaturesBatchMethod())
              .addMethod(getPingMethod())
              .addMethod(getSelectBestFrameMethod())
              .addMethod(getComputeFrameHashMethod())
              .addMethod(getDetectFaultsMethod())
              .addMethod(getGenerateEnhancementTextMethod())
              .addMethod(getOptimizeMaterialsMethod())
              .addMethod(getExtractVideoClipMethod())
              .addMethod(getGetModalityClassificationMethod())
              .addMethod(getGenerateScreenshotMethod())
              .build();
        }
      }
    }
    return result;
  }
}
