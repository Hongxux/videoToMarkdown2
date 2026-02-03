package com.videotomd.service;

import com.videotomd.config.JwtUtil;
import com.videotomd.dto.AuthResponse;
import com.videotomd.dto.LoginRequest;
import com.videotomd.dto.RegisterRequest;
import com.videotomd.entity.User;
import com.videotomd.repository.UserRepository;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 认证服务
 */
@Service
public class AuthService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtUtil jwtUtil;
    private final InviteCodeService inviteCodeService;

    public AuthService(UserRepository userRepository,
                       PasswordEncoder passwordEncoder,
                       JwtUtil jwtUtil,
                       InviteCodeService inviteCodeService) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.jwtUtil = jwtUtil;
        this.inviteCodeService = inviteCodeService;
    }

    /**
     * 用户注册
     */
    @Transactional
    public AuthResponse register(RegisterRequest request) {
        // 1. 验证邀请码
        if (!inviteCodeService.validateCode(request.getInviteCode())) {
            throw new RuntimeException("邀请码无效或已被使用");
        }

        // 2. 检查邮箱是否已存在
        if (userRepository.existsByEmail(request.getEmail())) {
            throw new RuntimeException("该邮箱已被注册");
        }

        // 3. 创建用户
        User user = new User();
        user.setEmail(request.getEmail());
        user.setPasswordHash(passwordEncoder.encode(request.getPassword()));
        user.setUsername(request.getUsername() != null ? request.getUsername() : request.getEmail().split("@")[0]);
        user.setRole(User.UserRole.USER);
        user.setInviteCode(request.getInviteCode());
        user.setEmailVerified(true);  // v1跳过邮箱验证

        User savedUser = userRepository.save(user);

        // 4. 标记邀请码已使用
        inviteCodeService.markAsUsed(request.getInviteCode(), savedUser.getUserId());

        // 5. 生成JWT
        String token = jwtUtil.generateToken(savedUser.getEmail(), savedUser.getUserId());

        return new AuthResponse(
                token,
                savedUser.getEmail(),
                savedUser.getUsername(),
                savedUser.getRole().name(),
                savedUser.getUserId()
        );
    }

    /**
     * 用户登录
     */
    public AuthResponse login(LoginRequest request) {
        // 1. 查找用户
        User user = userRepository.findByEmail(request.getEmail())
                .orElseThrow(() -> new RuntimeException("邮箱或密码错误"));

        // 2. 验证密码
        if (!passwordEncoder.matches(request.getPassword(), user.getPasswordHash())) {
            throw new RuntimeException("邮箱或密码错误");
        }

        // 3. 生成JWT
        String token = jwtUtil.generateToken(user.getEmail(), user.getUserId());

        return new AuthResponse(
                token,
                user.getEmail(),
                user.getUsername(),
                user.getRole().name(),
                user.getUserId()
        );
    }
}
