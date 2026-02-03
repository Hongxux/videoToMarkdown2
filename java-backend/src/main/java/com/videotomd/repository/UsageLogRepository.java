package com.videotomd.repository;

import com.videotomd.entity.UsageLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.Optional;

/**
 * 使用记录Repository
 */
@Repository
public interface UsageLogRepository extends JpaRepository<UsageLog, Long> {

    /**
     * 查找用户指定日期的使用记录
     */
    Optional<UsageLog> findByUserIdAndActionDate(Long userId, LocalDate actionDate);

    /**
     * 增加用户今日使用次数
     */
    @Modifying
    @Query("UPDATE UsageLog u SET u.actionCount = u.actionCount + 1 WHERE u.userId = :userId AND u.actionDate = :actionDate")
    int incrementUsageCount(@Param("userId") Long userId, @Param("actionDate") LocalDate actionDate);
}
