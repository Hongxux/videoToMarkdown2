package com.videotomd.repository;

import com.videotomd.entity.InviteCode;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * 邀请码Repository
 */
@Repository
public interface InviteCodeRepository extends JpaRepository<InviteCode, Long> {

    /**
     * 根据邀请码查找
     */
    Optional<InviteCode> findByInviteCode(String inviteCode);

    /**
     * 检查邀请码是否存在
     */
    boolean existsByInviteCode(String inviteCode);
}
