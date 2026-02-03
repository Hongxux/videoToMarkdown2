package com.videotomd.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * 注册请求DTO
 */
public class RegisterRequest {

    @NotBlank(message = "邀请码不能为空")
    private String inviteCode;

    @NotBlank(message = "邮箱不能为空")
    @Email(message = "邮箱格式不正确")
    private String email;

    @NotBlank(message = "密码不能为空")
    @Size(min = 6, max = 20, message = "密码长度必须在6-20之间")
    private String password;

    @Size(max = 100, message = "用户名长度不能超过100")
    private String username;

    public RegisterRequest() {
    }

    public RegisterRequest(String inviteCode, String email, String password, String username) {
        this.inviteCode = inviteCode;
        this.email = email;
        this.password = password;
        this.username = username;
    }

    public String getInviteCode() {
        return inviteCode;
    }

    public void setInviteCode(String inviteCode) {
        this.inviteCode = inviteCode;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}
