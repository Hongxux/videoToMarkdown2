package com.videotomd.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

/**
 * 邀请码实体类
 */
@Entity
@Table(name = "invite_codes")
public class InviteCode {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "code_id")
    private Long codeId;

    @Column(name = "invite_code", nullable = false, unique = true, length = 50)
    private String inviteCode;

    @Column(name = "created_by")
    private Long createdBy;

    @Column(name = "used_by")
    private Long usedBy;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "used_at")
    private LocalDateTime usedAt;

    @Column(name = "is_used", nullable = false)
    private Boolean isUsed = false;

    public InviteCode() {
    }

    public InviteCode(Long codeId, String inviteCode, Long createdBy, Long usedBy, LocalDateTime createdAt, LocalDateTime usedAt, Boolean isUsed) {
        this.codeId = codeId;
        this.inviteCode = inviteCode;
        this.createdBy = createdBy;
        this.usedBy = usedBy;
        this.createdAt = createdAt;
        this.usedAt = usedAt;
        this.isUsed = isUsed;
    }

    public Long getCodeId() {
        return codeId;
    }

    public void setCodeId(Long codeId) {
        this.codeId = codeId;
    }

    public String getInviteCode() {
        return inviteCode;
    }

    public void setInviteCode(String inviteCode) {
        this.inviteCode = inviteCode;
    }

    public Long getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(Long createdBy) {
        this.createdBy = createdBy;
    }

    public Long getUsedBy() {
        return usedBy;
    }

    public void setUsedBy(Long usedBy) {
        this.usedBy = usedBy;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUsedAt() {
        return usedAt;
    }

    public void setUsedAt(LocalDateTime usedAt) {
        this.usedAt = usedAt;
    }

    public Boolean getIsUsed() {
        return isUsed;
    }

    public void setIsUsed(Boolean isUsed) {
        this.isUsed = isUsed;
    }
}
