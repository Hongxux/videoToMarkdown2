package com.videotomd.repository;

import com.videotomd.entity.Task;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 任务Repository
 */
@Repository
public interface TaskRepository extends JpaRepository<Task, String> {

    /**
     * 查询用户的所有任务
     */
    List<Task> findByUserIdOrderByCreatedAtDesc(Long userId);

    /**
     * 根据状态查询任务
     */
    List<Task> findByStatus(Task.TaskStatus status);

    /**
     * 查询用户指定状态的任务
     */
    List<Task> findByUserIdAndStatus(Long userId, Task.TaskStatus status);

    /**
     * 统计用户在指定时间后的任务数量 (用于限流)
     */
    @Query("SELECT COUNT(t) FROM Task t WHERE t.userId = :userId AND t.createdAt >= :since")
    long countByUserIdAndCreatedAtAfter(@Param("userId") Long userId, @Param("since") LocalDateTime since);

    /**
     * 查找30天前完成的任务 (用于文件清理)
     */
    @Query("SELECT t FROM Task t WHERE t.status = 'COMPLETED' AND t.completedAt < :before AND t.resultPath IS NOT NULL")
    List<Task> findCompletedBefore(@Param("before") LocalDateTime before);
}
