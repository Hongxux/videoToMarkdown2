# gRPC 实施规范 (gRPC Implementation Specification)

**目标**: 实现 Java (编排端) 与 Python (工作端) 之间的同步通讯层，基于 `python和JAVA交互选型.md` 的分析结论，采用 gRPC + Protobuf 方案。

---

## 1. Protobuf 定义 (`fusion_service.proto`)

这是 Java 和 Python 之间严格的通信契约。

```protobuf
syntax = "proto3";

// 包名定义
package com.mvp.module2.fusion.grpc;

// Java 包配置
option java_multiple_files = true;
option java_package = "com.mvp.module2.fusion.grpc";
option java_outer_classname = "FusionServiceProto";

service FusionComputeService {
    // 1. 提交视频进行特征提取 (同步/阻塞)
    // Java 调用此接口触发 Python 处理
    rpc ExtractFeatures (FeatureRequest) returns (FeatureResponse);

    // 2. 健康检查 (用于 Java Sentinel/Resilience4j 熔断检测)
    rpc Ping (Empty) returns (HealthStatus);
}

message FeatureRequest {
    string request_id = 1;
    string video_path = 2;       // 本地共享路径 或 S3 URL (传引用)
    TimeRange time_range = 3;    // 可选: 仅处理特定片段
    AnalysisConfig config = 4;
}

message TimeRange {
    double start_sec = 1;
    double end_sec = 2;
}

message AnalysisConfig {
    bool enable_ocr = 1;
    bool enable_asr = 2;
    double mse_threshold = 3;    // 可选覆盖阈值
}

message FeatureResponse {
    string request_id = 1;
    bool success = 2;
    string error_message = 3;

    // 特征数据 (小元数据传值 + 大数据传引用)
    double duration_sec = 4;
    VisualFeatures visual_features = 5;
    
    // 对于截图、Tensor 等大数据，仅返回 Redis Key 或文件路径
    string result_redis_key = 6; 
}

message VisualFeatures {
    double avg_mse = 1;
    double avg_ssim = 2;
    bool is_potential_dynamic = 3;
    int32 element_count = 4;
}

message Empty {}

message HealthStatus {
    bool alive = 1;
    string cpu_load = 2;
    string gpu_memory_usage = 3;
}
```

---

## 2. Python Server 实施计划 (`grpc_server.py`)

**技术栈**: `grpcio`, `grpcio-tools`, `concurrent.futures`.

### 2.1 项目结构
```
python/
  protos/               # 生成的代码
  services/
     feature_extractor.py # 实际的逻辑封装 (Soldier)
  grpc_server.py        # 主入口
```

### 2.2 核心逻辑 (`grpc_server.py`)
```python
class FusionService(fusion_service_pb2_grpc.FusionComputeServiceServicer):
    def ExtractFeatures(self, request, context):
        try:
            # 1. 解析参数
            video_path = request.video_path
            
            # 2. 调用现有计算逻辑 (The Soldier)
            # 注意: 复用我们之前重构的无状态计算模块
            features = visual_extractor.process(video_path, request.time_range)
            
            # 3. 将重型结果存入 Redis (传引用)
            redis_key = f"features:{request.request_id}"
            redis.set(redis_key, pickle.dumps(features.heavy_data))
            
            # 4. 返回轻量级响应
            return FeatureResponse(
                request_id=request.request_id,
                success=True,
                visual_features=VisualFeatures(
                    avg_mse=features.avg_mse,
                    # ...
                ),
                result_redis_key=redis_key
            )
        except Exception as e:
            # 错误处理
            return FeatureResponse(success=False, error_message=str(e))
```

---

## 3. Java Client 实施计划 (`GrpcClientService.java`)

**技术栈**: `net.devh:grpc-server-spring-boot-starter` (或 client starter).

### 3.1 依赖 (Maven)
```xml
<dependency>
    <groupId>net.devh</groupId>
    <artifactId>grpc-client-spring-boot-starter</artifactId>
    <version>2.14.0.RELEASE</version>
</dependency>
```

### 3.2 服务封装
```java
@Service
public class PythonComputeClient {

    @GrpcClient("python-compute-service")
    private FusionComputeServiceBlockingStub blockingStub;

    // 特性 1: 弹性熔断 (Circuit Breaker)
    // 特性 2: 超时控制 (Deadline)
    public FeatureResponse extractFeatures(String videoPath) {
        try {
            FeatureRequest request = FeatureRequest.newBuilder()
                .setVideoPath(videoPath)
                .build();
                
            // 设置 30秒超时
            return blockingStub.withDeadlineAfter(30, TimeUnit.SECONDS)
                               .extractFeatures(request);
        } catch (StatusRuntimeException e) {
            // 处理 gRPC 特定错误 (UNAVAILABLE, DEADLINE_EXCEEDED) -> 触发熔断
            throw new ComputeServiceException("Python Worker Failed", e);
        }
    }
}
```

---

## 4. 开发工作流

1.  **定义**: 创建 `src/main/proto/fusion_service.proto`。
2.  **生成**: 运行 `mvn compile` (Java) 和 `python -m grpc_tools.protoc` (Python) 生成存根代码 (Stubs)。
3.  **实现**: 编写 Python 服务端逻辑和 Java 客户端封装。
4.  **验证**: 启动 Python server -> 运行 Java 单元测试调用 -> 断言响应结果。

## 5. 第一性原理检查 (First Principles Check)
*   **解耦 (Decoupling)**: 契约在 Proto 中定义，具体实现被隐藏。
*   **性能 (Performance)**: 二进制协议，极快的序列化速度。
*   **可靠性 (Reliability)**: 定义了显式的错误字段和健康检查端点 Ping。
