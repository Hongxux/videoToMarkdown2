package com.videotomd.service;

import com.videotomd.entity.InviteCode;
import com.videotomd.repository.InviteCodeRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

/**
 * 邀请码服务
 */
@Service
public class InviteCodeService {

    private final InviteCodeRepository inviteCodeRepository;

    public InviteCodeService(InviteCodeRepository inviteCodeRepository) {
        this.inviteCodeRepository = inviteCodeRepository;
    }

    /**
     * 生成邀请码
     */
    public InviteCode generateCode(Long createdBy) {
        String code = "INVITE-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        
        InviteCode inviteCode = new InviteCode();
        inviteCode.setInviteCode(code);
        inviteCode.setCreatedBy(createdBy);
        inviteCode.setIsUsed(false);
        
        return inviteCodeRepository.save(inviteCode);
    }

    /**
     * 验证邀请码
     */
    public boolean validateCode(String code) {
        Optional<InviteCode> inviteCode = inviteCodeRepository.findByInviteCode(code);
        return inviteCode.isPresent() && !inviteCode.get().getIsUsed();
    }

    /**
     * 标记邀请码已使用
     */
    @Transactional
    public void markAsUsed(String code, Long usedBy) {
        InviteCode inviteCode = inviteCodeRepository.findByInviteCode(code)
                .orElseThrow(() -> new RuntimeException("邀请码不存在"));
        
        if (inviteCode.getIsUsed()) {
            throw new RuntimeException("邀请码已被使用");
        }
        
        inviteCode.setIsUsed(true);
        inviteCode.setUsedBy(usedBy);
        inviteCode.setUsedAt(LocalDateTime.now());
        
        inviteCodeRepository.save(inviteCode);
    }
}
