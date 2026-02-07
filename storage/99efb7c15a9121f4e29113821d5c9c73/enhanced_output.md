# video

## CloudBot简介与特色

# CloudBot简介与特色

CloudBot 是一款近期迅速走红的开源 AI 助理，它运行在本地电脑上。在短短几天内，其 GitHub Star 数量直线飙升，已超过 12 万。

## 核心功能与定位
CloudBot 的功能与 CloudCode 和 OpenCode 有些类似，都能处理文件、编码、调用 skills、MCP 等，以协助用户处理工作。

## 主要特色
1.  **多平台接入**：CloudBot 最大的优势在于可以接入各种聊天工具。这意味着即使出门在外，手边没有电脑，用户也只需在聊天工具中给 CloudBot 留言，它还能将屏幕截图、执行过程等信息实时同步回来，非常方便。
2.  **智能定时与主动沟通**：CloudBot 自带了强大的定时器系统，用户只需使用自然语言就能创建定时器（例如创建临时提醒或定时检查收件箱）。相较于传统的固定指令或通知流程，CloudBot 具有很强的“主观能动性”，能够智能判断事情的紧急程度，并选择是否通过聊天工具与用户沟通。
3.  **长期记忆与进化**：CloudBot 具有长期记忆功能，可以将记忆作为文件存储在本地。在日常对话中，它能够搜索并调取相关的记忆到上下文中。随着日常使用，它还会主动更新这些记忆文件，给人一种越用越聪明的感觉。

## 补充说明
*   **名称变更**：由于受到法律压力，CloudBot 先后更名为 MultBot 和 OpenCloud。为保持一致性，本文仍使用其最初的名字“CloudBot”。
*   **扩展内容**：关于 CloudBot 的详细玩法案例（十几个）以及接入国产模型以在国内网络顺畅使用的方案，将在其他部分介绍。


### 部署环境选择与推荐

# 部署环境选择与推荐

## 概述
CloudBot 的部署环境需要能够运行 JavaScript。推荐使用 Mac 或 Linux 系统的家庭服务器进行部署![[assets/SU002/SU002_部署环境选择与推荐_img_01_routed_ss_su002_0.png]]。

## 推荐方案：Mac mini
目前最热门的部署方案是使用 Mac mini![[assets/SU002/SU002_部署环境选择与推荐_img_02_routed_ss_su002_1.png]]。

### 主要优点
1.  **系统与生态**：macOS 提供了良好的桌面环境，便于进行截图、操作浏览器等任务![[assets/SU002/SU002_部署环境选择与推荐_img_03_routed_ss_su002_2.png]]。同时，CloudBot 中的许多技能（skills）与 Mac 生态深度绑定![[assets/SU002/SU002_部署环境选择与推荐_img_04_routed_ss_su002_3.png]]。
2.  **功耗与运行**：设备功耗较低，适合 7x24 小时不间断运行，较为省电![[assets/SU002/SU002_部署环境选择与推荐_img_05_routed_ss_su002_4.png]]。

Supplemental images:
- unknown: ![[assets/SU002/SU002_部署环境选择与推荐_img_06_routed_ss_su002_5.png]]
- unknown: ![[assets/SU002/SU002_部署环境选择与推荐_img_07_routed_ss_su002_6.png]]
- unknown: ![[assets/SU002/SU002_部署环境选择与推荐_img_08_routed_ss_su002_7.png]]
- unknown: ![[assets/SU002/SU002_部署环境选择与推荐_img_09_routed_ss_su002_8.png]]


#### NodeJS安装步骤

# NodeJS安装步骤

## 概述
本流程描述了在非Mac环境下（如Linux或Windows中的Linux虚拟机）安装NodeJS的通用步骤![[assets/SU003/SU003_nodejs安装步骤_img_01_routed_ss_su003_0.png]]。

## 安装步骤
1.  **环境准备**
    *   如果没有Mac电脑，可以选择Linux操作系统，或者在Windows系统中创建一个Linux虚拟机![[assets/SU003/SU003_nodejs安装步骤_img_02_routed_ss_su003_1.png]]。

2.  **访问官网**
    *   访问NodeJS官方网站![[assets/SU003/SU003_nodejs安装步骤_img_03_routed_ss_su003_2.png]]。

3.  **执行安装命令**
    *   复制官网提供的第一个命令，在终端中执行![[assets/SU003/SU003_nodejs安装步骤_img_04_routed_ss_su003_3.png]]。
    *   依次执行官网提供的第二个和第三个命令![[assets/SU003/SU003_nodejs安装步骤_img_05_routed_ss_su003_4.png]]。

4.  **完成**
    *   完成上述命令后，NodeJS即安装完成![[assets/SU003/SU003_nodejs安装步骤_img_06_routed_ss_su003_5.png]]。

> Video **过程演示**

![[assets/SU003/SU003_nodejs安装步骤_clip_01_routed_clip_su003.mp4]]


### CloudBot安装与初始化

1. 1. 访问clawdbot官网，复制NPM一键安装命令并执行安装: from 157.00s to 183.80s
    - Keyframe 1 (8.50s): ![[vl_tutorial_units/SU004/SU004_step_01_clawdbot_npm_key.png]]
    - Step video: ![[vl_tutorial_units/SU004/SU004_step_01_clawdbot_npm.mp4]]

2. 2. 运行clawdbot onboard命令启动初始化配置，选择OpenAI作为模型提供方并使用ChatGPT OAuth登录账户 / 确认AI模型配置完成，选择默认模型gpt-5.2，并选择WhatsApp作为聊天通道: from 184.30s to 216.97s
    - Keyframe 1 (19.50s): ![[vl_tutorial_units/SU004/SU004_step_02_clawdbot_onboard_openai_chatgpt_oauth_ai_gpt_5_2_key.png]]
    - Keyframe 2 (24.50s): ![[vl_tutorial_units/SU004/SU004_step_02_clawdbot_onboard_openai_chatgpt_oauth_ai_gpt_5_2_key_02.png]]
    - Step video: ![[vl_tutorial_units/SU004/SU004_step_02_clawdbot_onboard_openai_chatgpt_oauth_ai_gpt_5_2.mp4]]

3. 3. 在手机WhatsApp中关联新设备：打开设置→已关联设备→关联新设备→扫描电脑端二维码完成绑定: from 217.47s to 225.47s
    - Keyframe 1 (33.00s): ![[vl_tutorial_units/SU004/SU004_step_03_whatsapp_key.png]]
    - Step video: ![[vl_tutorial_units/SU004/SU004_step_03_whatsapp.mp4]]

4. 4. 在终端中选择预装skills（如bird），跳过所有API Key配置，完成人设问答后进入主界面: from 225.97s to 239.63s
    - Keyframe 1 (46.50s): ![[vl_tutorial_units/SU004/SU004_step_04_skills_bird_api_key_key.png]]
    - Step video: ![[vl_tutorial_units/SU004/SU004_step_04_skills_bird_api_key.mp4]]

5. 5. 向聊天工具发送测试消息，验证手机端成功接收消息，确认整个配置流程完成: from 240.13s to 246.00s
    - Keyframe 1 (53.00s): ![[vl_tutorial_units/SU004/SU004_step_05_action_key.png]]
    - Step video: ![[vl_tutorial_units/SU004/SU004_step_05_action.mp4]]


### 基础命令使用介绍

# 基础命令使用介绍

## 启动与停止
*   输入命令 `CloudBot Gateway` 以启动主程序![[assets/SU005/SU005_基础命令使用介绍_img_01_routed_ss_su005_0.png]]。
*   可以通过关闭控制台窗口来停止 CloudBot 的运行![[assets/SU005/SU005_基础命令使用介绍_img_02_routed_ss_su005_1.png]]。

## 后台操作
*   当 CloudBot 在后台运行时，可以新开一个控制台窗口并输入命令 `CloudBot TOI`![[assets/SU005/SU005_基础命令使用介绍_img_03_routed_ss_su005_10.png]]。

## 网页控制台
*   输入命令 `CloudBot Dashboard` 可以进入网页版控制台![[assets/SU005/SU005_基础命令使用介绍_img_04_routed_ss_su005_11.png]]。
*   在网页控制台中，你可以进行基础对话和配置，以及管理定时任务和 Skills 等![[assets/SU005/SU005_基础命令使用介绍_img_05_routed_ss_su005_12.png]]。

## 账户管理
*   输入命令 `CloudBot Chat Logout` 可以退出在 CloudBot 上登录过的聊天软件![[assets/SU005/SU005_基础命令使用介绍_img_06_routed_ss_su005_13.png]]。
*   输入命令 `Login` 可以重新登录![[assets/SU005/SU005_基础命令使用介绍_img_07_routed_ss_su005_14.png]]。此命令可用于处理聊天软件可能出现的隔几天自动掉线的情况![[assets/SU005/SU005_基础命令使用介绍_img_08_routed_ss_su005_15.png]]。

Supplemental images:
- unknown: ![[assets/SU005/SU005_基础命令使用介绍_img_09_routed_ss_su005_16.png]]
- unknown: ![[assets/SU005/SU005_基础命令使用介绍_img_10_routed_ss_su005_17.png]]
- unknown: ![[assets/SU005/SU005_基础命令使用介绍_img_11_routed_ss_su005_2.png]]
- unknown: ![[assets/SU005/SU005_基础命令使用介绍_img_12_routed_ss_su005_3.png]]
- unknown: ![[assets/SU005/SU005_基础命令使用介绍_img_13_routed_ss_su005_4.png]]
- unknown: ![[assets/SU005/SU005_基础命令使用介绍_img_14_routed_ss_su005_5.png]]
- unknown: ![[assets/SU005/SU005_基础命令使用介绍_img_15_routed_ss_su005_6.png]]
- unknown: ![[assets/SU005/SU005_基础命令使用介绍_img_16_routed_ss_su005_7.png]]
- unknown: ![[assets/SU005/SU005_基础命令使用介绍_img_17_routed_ss_su005_8.png]]
- unknown: ![[assets/SU005/SU005_基础命令使用介绍_img_18_routed_ss_su005_9.png]]


#### 定时任务功能演示

# 定时任务功能演示

CloudBot 的定时任务功能是其最具特色的功能之一![[assets/SU006/SU006_定时任务功能演示_img_01_routed_ss_su006_0.png]]。该功能赋予了 CloudBot 一定的主观能动性，使其更像一个智能的 AE 助手![[assets/SU006/SU006_定时任务功能演示_img_02_routed_ss_su006_1.png]]。

**功能演示示例：**
例如，输入命令：“提醒我两分钟以后关闭它。”![[assets/SU006/SU006_定时任务功能演示_img_03_routed_ss_su006_10.png]]。CloudBot 会回复：“好的，已经设定成功了。”![[assets/SU006/SU006_定时任务功能演示_img_04_routed_ss_su006_2.png]]。

**查看与管理：**
随后，可以在网页版控制台的 “ChromeJob”（即定时任务）选项卡中，查看 CloudBot 设定的定时任务![[assets/SU006/SU006_定时任务功能演示_img_05_routed_ss_su006_3.png]]。此处会显示任务状态为“两分钟后执行”![[assets/SU006/SU006_定时任务功能演示_img_06_routed_ss_su006_4.png]]。

**任务执行：**
两分钟后，该提醒消息会被推送到手机上，提示用户去关闭设备![[assets/SU006/SU006_定时任务功能演示_img_07_routed_ss_su006_5.png]]。

Supplemental images:
- unknown: ![[assets/SU006/SU006_定时任务功能演示_img_08_routed_ss_su006_6.png]]
- unknown: ![[assets/SU006/SU006_定时任务功能演示_img_09_routed_ss_su006_7.png]]
- unknown: ![[assets/SU006/SU006_定时任务功能演示_img_10_routed_ss_su006_8.png]]
- unknown: ![[assets/SU006/SU006_定时任务功能演示_img_11_routed_ss_su006_9.png]]

> Video **过程演示**

![[assets/SU006/SU006_定时任务功能演示_clip_01_routed_clip_su006.mp4]]


#### 浏览器自动化案例

# 浏览器自动化案例

## 概述
这是一个使用 CloudBot 进行浏览器自动化操作的流程案例，展示了从下载浏览器、访问网站、搜索课程、下载课件到解压和发送文件的全过程![[assets/SU007/SU007_浏览器自动化案例_img_01_routed_ss_su007_0.png]]。

## 操作流程
1.  **环境准备**：首先需要在 Mac 系统中下载并安装 Chrome 浏览器![[assets/SU007/SU007_浏览器自动化案例_img_02_routed_ss_su007_1.png]]。
2.  **启动与导航**：CloudBot 自动打开 Mac 中的 Chrome 浏览器，访问 MIT 公开课官网，并执行“Python”关键词搜索![[assets/SU007/SU007_浏览器自动化案例_img_03_routed_ss_su007_10.png]]。
3.  **课程选择**：CloudBot 在网站上找到多个 Python 课程，并向用户反馈课程编号以供选择![[assets/SU007/SU007_浏览器自动化案例_img_04_routed_ss_su007_2.png]]。用户选择第一个课程后，CloudBot 根据课程编号在浏览器中定位到该课程页面![[assets/SU007/SU007_浏览器自动化案例_img_05_routed_ss_su007_3.png]]。
4.  **下载课件**：CloudBot 将所选 Python 课程的课件下载到用户的桌面![[assets/SU007/SU007_浏览器自动化案例_img_06_routed_ss_su007_4.png]]。
5.  **解压文件**：根据用户指令，CloudBot 调用 Mac 的命令行工具，将下载的压缩课件包解压![[assets/SU007/SU007_浏览器自动化案例_img_07_routed_ss_su007_5.png]]。
6.  **发送文件**：用户要求获取第一节课的课件，CloudBot 成功定位到该课件文件并将其发送给用户，完成了整个自动化任务![[assets/SU007/SU007_浏览器自动化案例_img_08_routed_ss_su007_6.png]]。

Supplemental images:
- unknown: ![[assets/SU007/SU007_浏览器自动化案例_img_09_routed_ss_su007_7.png]]
- unknown: ![[assets/SU007/SU007_浏览器自动化案例_img_10_routed_ss_su007_8.png]]
- unknown: ![[assets/SU007/SU007_浏览器自动化案例_img_11_routed_ss_su007_9.png]]

> Video **过程演示**

![[assets/SU007/SU007_浏览器自动化案例_clip_01_routed_clip_su007.mp4]]


#### 定时任务与浏览器组合案例

# 定时任务与浏览器组合案例

这是一个将浏览器自动化与定时任务相结合的案例![[assets/SU008/SU008_定时任务与浏览器组合案例_img_01_routed_ss_su008_0.png]]。

**场景**：博主“帕巴夏”需要经常查看 GitHub 热点![[assets/SU008/SU008_定时任务与浏览器组合案例_img_02_routed_ss_su008_1.png]]。

**流程**：
1.  首先，指示 CloudBot 查阅 GitHub 热点并生成一份中文简报![[assets/SU008/SU008_定时任务与浏览器组合案例_img_03_routed_ss_su008_2.png]]。
2.  CloudBot 成功生成了中文简报![[assets/SU008/SU008_定时任务与浏览器组合案例_img_04_routed_ss_su008_3.png]]。
3.  接着，提出要求：每天早晨 8 点自动执行此任务并发送简报![[assets/SU008/SU008_定时任务与浏览器组合案例_img_05_routed_ss_su008_4.png]]。
4.  CloudBot 据此创建了一个定时任务，计划在每天早晨 8 点执行![[assets/SU008/SU008_定时任务与浏览器组合案例_img_06_routed_ss_su008_5.png]]。

**结果**：
*   可以在 CloudBot 控制后台查看到此定时任务![[assets/SU008/SU008_定时任务与浏览器组合案例_img_07_routed_ss_su008_6.png]]。
*   该任务每天早晨 8 点会自动执行工作流程，生成并发送中文简报![[assets/SU008/SU008_定时任务与浏览器组合案例_img_08_routed_ss_su008_7.png]]。
*   整体效果良好![[assets/SU008/SU008_定时任务与浏览器组合案例_img_09_routed_ss_su008_8.png]]。

> Video **过程演示**

![[assets/SU008/SU008_定时任务与浏览器组合案例_clip_01_routed_clip_su008.mp4]]


### 图像识别功能配置与使用

# 图像识别功能配置与使用

除了操作浏览器，CloudBot 还具备图像识别等 AI 视觉能力![[assets/SU009/SU009_图像识别功能配置与使用_img_01_routed_ss_su009_0.png]]。

要启用此功能，需进行以下配置：
1.  打开 Mac mini 的系统设置![[assets/SU009/SU009_图像识别功能配置与使用_img_02_routed_ss_su009_1.png]]。
2.  进入“隐私与安全”设置中的“屏幕与系统录音”权限管理页面![[assets/SU009/SU009_图像识别功能配置与使用_img_03_routed_ss_su009_2.png]]。
3.  在此页面搜索“终端”二字![[assets/SU009/SU009_图像识别功能配置与使用_img_04_routed_ss_su009_3.png]]。
4.  为命令行终端（Terminal）应用程序添加屏幕录制和录音权限![[assets/SU009/SU009_图像识别功能配置与使用_img_05_routed_ss_su009_4.png]]。

配置完成后，重启 CloudBot 服务![[assets/SU009/SU009_图像识别功能配置与使用_img_06_routed_ss_su009_5.png]]。

**功能使用示例：**
在手机端对 CloudBot 说出指令：“请给现在的 Mac 截一个图”![[assets/SU009/SU009_图像识别功能配置与使用_img_07_routed_ss_su009_6.png]]。
随后，Mac 电脑当前的屏幕截图便会实时发送到手机上![[assets/SU009/SU009_图像识别功能配置与使用_img_08_routed_ss_su009_7.png]]。
通过此功能，可以实现对 Mac 电脑状态的实时监控![[assets/SU009/SU009_图像识别功能配置与使用_img_09_routed_ss_su009_8.png]]。

> Video **过程演示**

![[assets/SU009/SU009_图像识别功能配置与使用_clip_01_routed_clip_su009.mp4]]


### 接入飞书国内聊天工具

1. 1. 在飞书开放平台创建企业自建应用，填写应用名称与描述，并选择图标: from 425.00s to 430.00s
    - Keyframe 1 (13.80s): ![[vl_tutorial_units/SU010/SU010_step_01_action_key.png]]
    - Step video: ![[vl_tutorial_units/SU010/SU010_step_01_action.mp4]]

2. 2. 添加应用能力并配置机器人权限：进入‘添加应用能力’，选择‘机器人’，再进入‘权限管理’点击‘开通权限’，勾选所需权限项 / 完成版本管理与发布：填写应用版本号（如1.0.0），提交并点击‘发布’按钮完成上线: from 431.00s to 444.00s
    - Keyframe 1 (22.00s): ![[vl_tutorial_units/SU010/SU010_step_02_1_0_0_key.png]]
    - Keyframe 2 (27.80s): ![[vl_tutorial_units/SU010/SU010_step_02_1_0_0_key_02.png]]
    - Step video: ![[vl_tutorial_units/SU010/SU010_step_02_1_0_0.mp4]]

3. 3. 在Mac终端安装飞书插件并配置APP ID：执行clawdbot plugins install命令安装插件；从飞书开放平台复制App ID，执行config set channels.feishu.appId命令配置: from 445.00s to 456.40s
    - Keyframe 1 (39.50s): ![[vl_tutorial_units/SU010/SU010_step_03_mac_app_id_clawdbot_plugins_install_app_id_confi_key.png]]
    - Step video: ![[vl_tutorial_units/SU010/SU010_step_03_mac_app_id_clawdbot_plugins_install_app_id_confi.mp4]]

4. 4. 在Mac终端配置APP Secret与连接模式：从飞书平台复制App Secret，执行config set channels.feishu.appSecret命令；设置enabled为true并修改connectionMode为websocket: from 457.40s to 468.87s
    - Keyframe 1 (51.50s): ![[vl_tutorial_units/SU010/SU010_step_04_mac_app_secret_app_secret_config_set_channels_fe_key.png]]
    - Step video: ![[vl_tutorial_units/SU010/SU010_step_04_mac_app_secret_app_secret_config_set_channels_fe.mp4]]

5. 5. 重启clawbot服务并汇总全部配置命令供用户截图保存: from 469.87s to 476.13s
    - Keyframe 1 (57.50s): ![[vl_tutorial_units/SU010/SU010_step_05_clawbot_key.png]]
    - Step video: ![[vl_tutorial_units/SU010/SU010_step_05_clawbot.mp4]]

6. 6. 在飞书开放平台配置事件回调：选择‘长连接’订阅方式，点击编辑添加‘接收消息’事件，保存后创建新版本并提交发布: from 477.13s to 490.13s
    - Keyframe 1 (71.00s): ![[vl_tutorial_units/SU010/SU010_step_06_action_key.png]]
    - Step video: ![[vl_tutorial_units/SU010/SU010_step_06_action.mp4]]

7. 7. 在飞书手机App中测试机器人功能：打开‘MoltBot机器人’会话，发送‘你好’和‘现在几点了’验证基础交互与时间响应: from 491.13s to 502.13s
    - Keyframe 1 (83.50s): ![[vl_tutorial_units/SU010/SU010_step_07_app_moltbot_key.png]]
    - Step video: ![[vl_tutorial_units/SU010/SU010_step_07_app_moltbot.mp4]]

8. 8. 处理截屏权限并完成文件传递：收到机器人提示需先授权；跳转至Mac控制台对话确认设备所有权后，返回App成功获取截图并接收文件: from 503.13s to 516.13s
    - Keyframe 1 (97.00s): ![[vl_tutorial_units/SU010/SU010_step_08_mac_app_key.png]]
    - Step video: ![[vl_tutorial_units/SU010/SU010_step_08_mac_app.mp4]]


#### 切换国产AI模型

# 切换国产AI模型

## 概述
本流程指导如何将 CloudBot 的 AI 模型切换至国产模型 minimax。

## 详细步骤

1.  **获取 minimax API 密钥**
    *   访问 minimax 开放平台![[assets/SU011/SU011_切换国产ai模型_img_01_routed_ss_su011_0.png]]。
    *   在平台左侧导航栏选择“接口密钥”![[assets/SU011/SU011_切换国产ai模型_img_02_routed_ss_su011_1.png]]。
    *   创建一个新的 API-K (API 密钥) ![[assets/SU011/SU011_切换国产ai模型_img_03_routed_ss_su011_10.png]]。

2.  **在 CloudBot 中配置 minimax**
    *   打开 Mac 的终端（控制台）![[assets/SU011/SU011_切换国产ai模型_img_04_routed_ss_su011_11.png]]。
    *   输入命令 `CloudBot Config` 以启动配置程序![[assets/SU011/SU011_切换国产ai模型_img_05_routed_ss_su011_12.png]]。
    *   在配置过程中，选择 `minimax` 作为模型![[assets/SU011/SU011_切换国产ai模型_img_06_routed_ss_su011_13.png]]。
    *   将步骤 1 中创建的 minimax API-K 填写到对应配置项中![[assets/SU011/SU011_切换国产ai模型_img_07_routed_ss_su011_2.png]]。
    *   后续选项可一路按回车使用默认值，直至配置完成![[assets/SU011/SU011_切换国产ai模型_img_08_routed_ss_su011_3.png]]。

3.  **在 CloudBot 控制台选择模型**
    *   打开 CloudBot 的控制台![[assets/SU011/SU011_切换国产ai模型_img_09_routed_ss_su011_4.png]]。
    *   输入命令 `/models` ![[assets/SU011/SU011_切换国产ai模型_img_10_routed_ss_su011_5.png]]。
    *   从模型列表中选择 `minimax` 模型![[assets/SU011/SU011_切换国产ai模型_img_11_routed_ss_su011_6.png]]。

4.  **完成切换**
    *   选择模型后，重启 CloudBot 应用![[assets/SU011/SU011_切换国产ai模型_img_12_routed_ss_su011_7.png]]。
    *   重启后，AI 模型即切换为 minimax![[assets/SU011/SU011_切换国产ai模型_img_13_routed_ss_su011_8.png]]。

Supplemental images:
- unknown: ![[assets/SU011/SU011_切换国产ai模型_img_14_routed_ss_su011_9.png]]

> Video **过程演示**

![[assets/SU011/SU011_切换国产ai模型_clip_01_routed_clip_su011.mp4]]


### 接入谷歌生态Skills

1. 1. 介绍clawdbot的skills功能并演示在Gateway Dashboard中一键安装第三方技能（如gog、github等） / 在Google Cloud Console中导航至API与服务 > 凭证 > OAuth权限请求页面，创建OAuth客户端ID，选择应用类型为桌面应用并命名: from 556.00s to 585.50s
    - Keyframe 1 (5.00s): ![[vl_tutorial_units/SU012/SU012_step_01_clawdbot_skills_gateway_dashboard_gog_github_goo_key.png]]
    - Keyframe 2 (14.00s): ![[vl_tutorial_units/SU012/SU012_step_01_clawdbot_skills_gateway_dashboard_gog_github_goo_key_02.png]]
    - Step video: ![[vl_tutorial_units/SU012/SU012_step_01_clawdbot_skills_gateway_dashboard_gog_github_goo.mp4]]

2. 2. 下载生成的client_secret.json文件，并将其拖入clawdbot终端进行gog认证配置 / 登录Google账号并授予gog访问权限（包括Gmail、Calendar、Docs等），完成授权后返回终端确认连接成功: from 586.00s to 597.50s
    - Keyframe 1 (21.00s): ![[vl_tutorial_units/SU012/SU012_step_02_client_secret_json_clawdbot_gog_google_gog_gmail_key.png]]
    - Keyframe 2 (26.00s): ![[vl_tutorial_units/SU012/SU012_step_02_client_secret_json_clawdbot_gog_google_gog_gmail_key_02.png]]
    - Step video: ![[vl_tutorial_units/SU012/SU012_step_02_client_secret_json_clawdbot_gog_google_gog_gmail.mp4]]

3. 3. 在clawdbot终端中查询最近邮件并总结内容；当遇到403权限不足时，按提示启用Gmail API并等待生效，再次执行命令成功读取邮件摘要 / 指令clawdbot将所有邮件移动到垃圾箱，系统提供两种方案并确认选择方案1，执行后显示已移动43封邮件完成操作 / 通过手机WhatsApp向clawdbot发送指令，要求: from 598.00s to 621.07s
    - Keyframe 1 (33.50s): ![[vl_tutorial_units/SU012/SU012_step_03_clawdbot_403_gmail_api_clawdbot_1_43_whatsapp_cl_key.png]]
    - Keyframe 2 (38.00s): ![[vl_tutorial_units/SU012/SU012_step_03_clawdbot_403_gmail_api_clawdbot_1_43_whatsapp_cl_key_02.png]]
    - Keyframe 3 (42.00s): ![[vl_tutorial_units/SU012/SU012_step_03_clawdbot_403_gmail_api_clawdbot_1_43_whatsapp_cl_key_03.png]]
    - Step video: ![[vl_tutorial_units/SU012/SU012_step_03_clawdbot_403_gmail_api_clawdbot_1_43_whatsapp_cl.mp4]]

4. 4. 设置clawdbot每2分钟检查Gmail未读邮件，发现新邮件即通过WhatsApp通知；随后从Gmail发送测试邮件，clawdbot成功检测并推送通知含发件人与主题 / 在WhatsApp中要求clawdbot总结新邮件内容，其返回包含版本号、标题及Bug Fixes列表的结构化摘要，确认任务完成: from 621.57s to 632.07s
    - Keyframe 1 (51.00s): ![[vl_tutorial_units/SU012/SU012_step_04_clawdbot_2_gmail_whatsapp_gmail_clawdbot_whatsap_key.png]]
    - Keyframe 2 (53.00s): ![[vl_tutorial_units/SU012/SU012_step_04_clawdbot_2_gmail_whatsapp_gmail_clawdbot_whatsap_key_02.png]]
    - Step video: ![[vl_tutorial_units/SU012/SU012_step_04_clawdbot_2_gmail_whatsapp_gmail_clawdbot_whatsap.mp4]]


#### 配置MCP对接百度地图

# 配置MCP对接百度地图

## 概述
AI助手通过MCP（Model Context Protocol）对接外部生态是一个重要渠道![[assets/SU013/SU013_配置mcp对接百度地图_img_01_routed_ss_su013_0.png]]。

## 配置步骤
1.  **安装MCP工具**：在AI助手的skills（技能）中找到并安装名为`mcpporter`的skill![[assets/SU013/SU013_配置mcp对接百度地图_img_02_routed_ss_su013_1.png]]。
2.  **配置百度地图MCP**：指示AI使用`mcpporter`来配置百度地图的MCP，并向其提供该MCP的说明文档![[assets/SU013/SU013_配置mcp对接百度地图_img_03_routed_ss_su013_10.png]]。
3.  **选择安装方案**：AI提供了多种安装方案，选择“DreamableHDDP”方案进行安装![[assets/SU013/SU013_配置mcp对接百度地图_img_04_routed_ss_su013_11.png]]。
4.  **提供API密钥**：根据要求，提供百度地图的API Key（AK）给AI![[assets/SU013/SU013_配置mcp对接百度地图_img_05_routed_ss_su013_12.png]]。完成此步骤后，配置即告完成，AI已能使用该MCP查询地理位置坐标![[assets/SU013/SU013_配置mcp对接百度地图_img_06_routed_ss_su013_13.png]]。

## 功能验证
- **查询路线**：指示AI查询从“青岛太平角公园”到“崂山仰口景区”的路线![[assets/SU013/SU013_配置mcp对接百度地图_img_07_routed_ss_su013_14.png]]。
- **验证结果**：AI成功返回了路线规划结果，效果良好![[assets/SU013/SU013_配置mcp对接百度地图_img_08_routed_ss_su013_2.png]]。

## 补充说明
该配置完成后，同样可在手机端使用![[assets/SU013/SU013_配置mcp对接百度地图_img_09_routed_ss_su013_3.png]]。

Supplemental images:
- unknown: ![[assets/SU013/SU013_配置mcp对接百度地图_img_10_routed_ss_su013_4.png]]
- unknown: ![[assets/SU013/SU013_配置mcp对接百度地图_img_11_routed_ss_su013_5.png]]
- unknown: ![[assets/SU013/SU013_配置mcp对接百度地图_img_12_routed_ss_su013_6.png]]
- unknown: ![[assets/SU013/SU013_配置mcp对接百度地图_img_13_routed_ss_su013_7.png]]
- unknown: ![[assets/SU013/SU013_配置mcp对接百度地图_img_14_routed_ss_su013_8.png]]
- unknown: ![[assets/SU013/SU013_配置mcp对接百度地图_img_15_routed_ss_su013_9.png]]

> Video **过程演示**

![[assets/SU013/SU013_配置mcp对接百度地图_clip_01_routed_clip_su013.mp4]]


#### 使用coding agent编程案例

# 使用coding agent编程案例

我们再来看一个 skills 的使用![[assets/SU014/SU014_使用coding_agent编程案例_img_01_routed_ss_su014_0.png]]。这里有一个 skills 叫做 coding agent，它可以驱动本地的 codex、cloud code、open code 等 AI 编程工具直接进行编程![[assets/SU014/SU014_使用coding_agent编程案例_img_02_routed_ss_su014_1.png]]。

这里我们先把这个 skills 安装一下![[assets/SU014/SU014_使用coding_agent编程案例_img_03_routed_ss_su014_10.png]]。我在 Mac 电脑上登录了我的 codex![[assets/SU014/SU014_使用coding_agent编程案例_img_04_routed_ss_su014_2.png]]，接着我在手机上跟 AI 说，要用 codex 创建一个贪吃蛇的游戏![[assets/SU014/SU014_使用coding_agent编程案例_img_05_routed_ss_su014_3.png]]。

我们看到程序编写好了![[assets/SU014/SU014_使用coding_agent编程案例_img_06_routed_ss_su014_4.png]]，通过 cloud bolt 驱动 Codex 完成了一个程序的开发![[assets/SU014/SU014_使用coding_agent编程案例_img_07_routed_ss_su014_5.png]]。

Supplemental images:
- unknown: ![[assets/SU014/SU014_使用coding_agent编程案例_img_08_routed_ss_su014_6.png]]
- unknown: ![[assets/SU014/SU014_使用coding_agent编程案例_img_09_routed_ss_su014_7.png]]
- unknown: ![[assets/SU014/SU014_使用coding_agent编程案例_img_10_routed_ss_su014_8.png]]
- unknown: ![[assets/SU014/SU014_使用coding_agent编程案例_img_11_routed_ss_su014_9.png]]

> Video **过程演示**

![[assets/SU014/SU014_使用coding_agent编程案例_clip_01_routed_clip_su014.mp4]]

