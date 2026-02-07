# video

## CloudBot简介与特色

# CloudBot简介与特色

CloudBot 是一款近期迅速走红的开源 AI 助理，运行于本地电脑。其在 GitHub 上的 Star 数量在短时间内急剧增长，已超过 12 万。

> **名称变更说明**：由于受到法律压力，该项目先后更名为 MultBot 和 OpenCloud。为保持一致性，本文仍沿用其最初名称 **CloudBot**。

## 核心功能与定位
CloudBot 的功能与 CloudCode、OpenCode 类似，能够：
*   处理文件。
*   编写代码。
*   调用技能（skills）及 MCP 等工具以协助处理工作。

## 主要特色与优势

### 1. 多平台接入与远程协同
CloudBot 最大的优势在于能够接入各种聊天工具。即使出门在外，身边没有电脑，用户也可以通过聊天工具向 CloudBot 发送指令，并能实时接收屏幕截图、任务执行过程等同步信息，非常便捷。

### 2. 强大的自然语言定时器系统
CloudBot 内置了强大的定时器功能，用户只需使用自然语言即可创建定时任务，例如：
*   设置临时提醒。
*   定时检查收件箱。

### 3. 智能化的主观能动性
相较于依赖固定指令或通知流程的传统工具，CloudBot 具备更强的自主性。它能够智能判断任务的紧急程度，并自主决定是否通过聊天工具与用户进行沟通。

### 4. 持久的长期记忆能力
CloudBot 拥有长期记忆功能，可以将记忆以文件形式存储在本地。在日常对话中，它能主动搜索并调用相关的记忆到当前上下文中。随着持续使用，它还会主动更新这些记忆文件，从而带来“越用越聪明”的体验。


### 部署环境选择与推荐

部署环境选择与推荐:
	部署前提: 任意能够运行JavaScript的环境都可以部署CloudBot。
	推荐方案: 使用Mac或Linux系统的家庭服务器。
		当前流行方案: 使用Mac mini。
			优势:
				- 桌面环境良好: 便于进行截图、操作浏览器等任务。
				- 功耗较低: 适合7x24小时运行，节省电力。
				- 生态系统绑定: CloudBot中的许多skills与Mac生态系统是绑定的。


#### NodeJS安装步骤

前提条件:
	- 如果没有Mac电脑，可以选择Linux操作系统
	- 或者在Windows系统中创建一个Linux虚拟机
安装方式:
	- 选择NodeJS安装
操作步骤:
	- 访问NodeJS官网
	- 复制第一个命令
	- 打开终端执行第一个命令
	- 执行第二个命令
	- 执行第三个命令
结果:
	- NodeJS安装完成


### CloudBot安装与初始化

1. 1. 访问clawdbot官网并复制NPM一键安装命令，在终端执行安装: from 157.00s to 183.80s
    - Keyframe 1 (8.50s): ![[vl_tutorial_units/SU004/SU004_step_01_clawdbot_npm_key.png]]
    - Step video: ![[vl_tutorial_units/SU004/SU004_step_01_clawdbot_npm.mp4]]

2. 2. 执行clawdbot onboarding初始化，配置AI模型为OpenAI并选择ChatGPT OAuth登录方式: from 184.30s to 209.30s
    - Keyframe 1 (22.00s): ![[vl_tutorial_units/SU004/SU004_step_02_clawdbot_onboarding_ai_openai_chatgpt_oauth_key.png]]
    - Step video: ![[vl_tutorial_units/SU004/SU004_step_02_clawdbot_onboarding_ai_openai_chatgpt_oauth.mp4]]

3. 3. 选择WhatsApp作为聊天通道，并在手机端完成设备关联：打开WhatsApp > 已关联设备 > 关联新设备 > 扫描电脑端二维码: from 215.97s to 225.47s
    - Keyframe 1 (33.00s): ![[vl_tutorial_units/SU004/SU004_step_03_whatsapp_whatsapp_key.png]]
    - Step video: ![[vl_tutorial_units/SU004/SU004_step_03_whatsapp_whatsapp.mp4]]

4. 4. 选择预装skills（如bird），跳过所有API Key配置，完成人设问答后启动Agent: from 225.97s to 239.63s
    - Keyframe 1 (46.50s): ![[vl_tutorial_units/SU004/SU004_step_04_skills_bird_api_key_agent_key.png]]
    - Step video: ![[vl_tutorial_units/SU004/SU004_step_04_skills_bird_api_key_agent.mp4]]

5. 5. 向聊天工具发送测试消息，验证配置成功：在终端输入指令后，手机WhatsApp收到消息并显示已上线: from 240.13s to 246.00s
    - Keyframe 1 (53.00s): ![[vl_tutorial_units/SU004/SU004_step_05_whatsapp_key.png]]
    - Step video: ![[vl_tutorial_units/SU004/SU004_step_05_whatsapp.mp4]]


### 基础命令使用介绍

基础命令使用介绍:
	目的: 介绍几个基础命令的使用方法
	命令列表:
		- CloudBot Gateway:
			功能: 启动主程序
		- 关闭控制台:
			功能: 停止 CloudBot 的运行
		- CloudBot TOI:
			前提条件: CloudBot 在后台运行时
			操作: 新开一个窗口并输入命令
		- CloudBot Dashboard:
			功能: 进入网页版控制台
			控制台功能:
				- 进行基础对话和配置
				- 管理定时任务和 Skills 等
		- CloudBot Chat Logout:
			功能: 退出 CloudBot 上登录过的聊天软件
		- Login:
			功能: 重新登录聊天软件
			原因: 有的聊天软件可能隔几天就会掉线
			操作: 使用这个命令来重新登录


#### 定时任务功能演示

功能概述: 定时任务功能演示
	核心特性: 定时任务赋予CloudBot主观能动性，使其更类似于智能AE助手
	操作示例:
		- 用户输入命令: 提醒我两分钟以后关闭它
		- CloudBot回复: 好的，已经设定成功了
		- 用户查看任务: 在网页版控制台的ChromeJob（定时任务）选项卡中，可看到设定的定时任务，显示为两分钟后执行
		- 任务执行结果: 两分钟后，CloudBot将消息推送至手机，提醒用户关闭设备


#### 浏览器自动化案例

能力概述: CloudBot具有操作浏览器的能力。
	前提条件: 用户需要在Mac系统中下载并安装一个Chrome浏览器。
	自动化操作流程:
		- 启动浏览器: CloudBot自动打开了Mac系统中的Chrome浏览器。
		- 访问网站: CloudBot访问了MIT公开课的官方网站。
		- 执行搜索: CloudBot执行了Python关键词的搜索。
		- 课程发现: CloudBot找到了多门Python课程。
			- 用户交互: CloudBot向用户回复了课程的编号，并请求用户选择。
				- 用户选择: 用户选择了第一个课程。
		- 课程定位: CloudBot在浏览器中通过课程编号定位到该课程。
		- 下载课件: CloudBot下载课件到桌面。
		- 解压操作: 用户要求CloudBot解压课件。
			- 工具调用: CloudBot调用了Mac的命令行工具完成了解压操作。
		- 发送课件: 用户要求CloudBot发送第一节课的课件。
			- 执行结果: CloudBot成功找到了课件并完成了发送，效果良好。


#### 定时任务与浏览器组合案例

案例介绍: 浏览器自动化与定时任务组合案例
	背景: 帕巴夏作为科技软件类博主，需要经常查看GitHub的热点
	任务指令: 我告诉CloudBot查阅GitHub热点并生成中文简报发送给我
		执行结果: CloudBot生成了中文简报
	定时任务设置: 我要求CloudBot每天早晨8点执行简报生成和发送任务
		执行结果: CloudBot生成了一个定时任务
			定时任务属性: 每天早晨8点执行
				执行内容: 生成中文简报并发送给我
	监控与效果: 我们可以在CloudBot控制后台查找到定时任务
		执行效果: 每天早晨8点自动执行工作流程发送简报，效果不错


### 图像识别功能配置与使用

功能概述:
	CloudBot具备AI视觉能力，包括图像识别。
配置步骤:
	- 打开Mac mini的设置。
	- 进入隐私与安全设置中的屏幕与系统录音选项。
	- 搜索“终端”两个字。
	- 为命令行终端添加屏幕和录音权限。
	- 重启CloudBot。
使用示例:
	- 在手机中说“请给现在的Mac截一个图”。
	- Mac电脑当前的图片发送到手机上。
	- 实现对状态的实时监控。


### 接入飞书国内聊天工具

1. 1. 在飞书开放平台创建企业应用，填写应用名称与描述，并添加机器人能力: from 425.00s to 433.00s
    - Keyframe 1 (13.50s): ![[vl_tutorial_units/SU010/SU010_step_01_action_key.png]]
    - Keyframe 2 (16.80s): ![[vl_tutorial_units/SU010/SU010_step_01_action_key_02.png]]
    - Step video: ![[vl_tutorial_units/SU010/SU010_step_01_action.mp4]]

2. 2. 进入权限管理页面，开通机器人所需各项权限（如获取用户信息、接收消息等） / 配置应用版本号并发布应用，完成基础应用设置: from 434.00s to 444.00s
    - Keyframe 1 (20.50s): ![[vl_tutorial_units/SU010/SU010_step_02_action_key.png]]
    - Keyframe 2 (23.80s): ![[vl_tutorial_units/SU010/SU010_step_02_action_key_02.png]]
    - Keyframe 3 (27.00s): ![[vl_tutorial_units/SU010/SU010_step_02_action_key_03.png]]
    - Keyframe 4 (27.80s): ![[vl_tutorial_units/SU010/SU010_step_02_action_key_04.png]]
    - Step video: ![[vl_tutorial_units/SU010/SU010_step_02_action.mp4]]

3. 3. 在Mac终端安装飞书插件，并配置APP ID、APP Secret、启用飞书通道及WebSocket连接模式: from 445.00s to 470.87s
    - Keyframe 1 (31.00s): ![[vl_tutorial_units/SU010/SU010_step_03_mac_app_id_app_secret_websocket_key.png]]
    - Keyframe 2 (40.00s): ![[vl_tutorial_units/SU010/SU010_step_03_mac_app_id_app_secret_websocket_key_02.png]]
    - Keyframe 3 (47.00s): ![[vl_tutorial_units/SU010/SU010_step_03_mac_app_id_app_secret_websocket_key_03.png]]
    - Keyframe 4 (53.50s): ![[vl_tutorial_units/SU010/SU010_step_03_mac_app_id_app_secret_websocket_key_04.png]]
    - Step video: ![[vl_tutorial_units/SU010/SU010_step_03_mac_app_id_app_secret_websocket.mp4]]

4. 4. 在飞书开放平台配置事件回调：选择长连接方式，添加接收消息事件，并创建新版本提交发布: from 477.13s to 490.13s
    - Keyframe 1 (61.50s): ![[vl_tutorial_units/SU010/SU010_step_04_action_key.png]]
    - Keyframe 2 (64.50s): ![[vl_tutorial_units/SU010/SU010_step_04_action_key_02.png]]
    - Keyframe 3 (68.50s): ![[vl_tutorial_units/SU010/SU010_step_04_action_key_03.png]]
    - Keyframe 4 (71.50s): ![[vl_tutorial_units/SU010/SU010_step_04_action_key_04.png]]
    - Step video: ![[vl_tutorial_units/SU010/SU010_step_04_action.mp4]]

5. 5. 在飞书App中与MoltBot机器人交互测试：发送问候、查询时间、请求截图；因权限限制需先在Clawdbot控制台确认设备所有权以开通截图权限: from 491.13s to 516.13s
    - Keyframe 1 (77.00s): ![[vl_tutorial_units/SU010/SU010_step_05_app_moltbot_clawdbot_key.png]]
    - Keyframe 2 (83.50s): ![[vl_tutorial_units/SU010/SU010_step_05_app_moltbot_clawdbot_key_02.png]]
    - Keyframe 3 (91.50s): ![[vl_tutorial_units/SU010/SU010_step_05_app_moltbot_clawdbot_key_03.png]]
    - Keyframe 4 (95.50s): ![[vl_tutorial_units/SU010/SU010_step_05_app_moltbot_clawdbot_key_04.png]]
    - Step video: ![[vl_tutorial_units/SU010/SU010_step_05_app_moltbot_clawdbot.mp4]]


### 切换国产AI模型

切换国产AI模型:
	背景:
		- 聊天方式切换为国内平台
		- AI模型切换为国内平台
		- CloudBot的作者推荐使用minimax
	步骤:
		- 获取API密钥:
			- 访问minimax开放平台
			- 在左侧选择接口密钥
			- 创建一个API-K
		- 配置CloudBot:
			- 打开Mac的控制台
			- 输入CloudBot Config命令
			- 模型选择minimax
			- 填写minimax API-K
			- 一路回车完成配置
		- 选择模型:
			- 访问CloudBot的控制台
			- 输入命令斜杠models
			- 选择minimax模型
			- 填写minimax API-K
			- 一路回车完成配置
		- 完成切换:
			- 选择模型后
			- 重启CloudBot
			- 模型切换完成


#### 接入谷歌生态Skills

1. 1. 在Clawdbot Gateway Dashboard中浏览内置Skills列表，说明其可接入第三方生态（如gog、github等），并演示点击'Install'按钮一键安装技能（如gog）。 / 进入Google Cloud Console，导航至API与服务 > 凭证 > OAuth权限请求页面，创建OAuth客户: from 556.00s to 585.50s
    - Keyframe 1 (5.50s): ![[vl_tutorial_units/SU012/SU012_step_01_clawdbot_gateway_dashboard_skills_gog_github_ins_key.png]]
    - Keyframe 2 (14.20s): ![[vl_tutorial_units/SU012/SU012_step_01_clawdbot_gateway_dashboard_skills_gog_github_ins_key_02.png]]
    - Step video: ![[vl_tutorial_units/SU012/SU012_step_01_clawdbot_gateway_dashboard_skills_gog_github_ins.mp4]]

2. 2. 保存生成的client_secret.json文件，并将其拖入Clawdbot终端界面，执行配置命令以启用gog认证。 / 在浏览器中登录Google账号并授予Clawdbot访问Gmail、Calendar、Docs等权限，完成OAuth授权流程，返回终端确认已连接。: from 586.00s to 597.50s
    - Keyframe 1 (21.00s): ![[vl_tutorial_units/SU012/SU012_step_02_client_secret_json_clawdbot_gog_google_clawdbot_key.png]]
    - Keyframe 2 (26.00s): ![[vl_tutorial_units/SU012/SU012_step_02_client_secret_json_clawdbot_gog_google_clawdbot_key_02.png]]
    - Step video: ![[vl_tutorial_units/SU012/SU012_step_02_client_secret_json_clawdbot_gog_google_clawdbot.mp4]]

3. 3. 在Clawdbot终端中输入指令查询最近邮件并要求总结；系统提示Gmail API未启用，按提示开启Gmail API后重试，成功返回近7天邮件概览及内容摘要。 / 指令Clawdbot将所有邮件移动至垃圾箱，系统提供两种方案供选择，用户选择方案1后，Clawdbot执行批量移动操作并反馈结果（共43封）。 / 通过手: from 598.00s to 621.07s
    - Keyframe 1 (33.50s): ![[vl_tutorial_units/SU012/SU012_step_03_clawdbot_gmail_api_gmail_api_7_clawdbot_1_clawdb_key.png]]
    - Keyframe 2 (38.00s): ![[vl_tutorial_units/SU012/SU012_step_03_clawdbot_gmail_api_gmail_api_7_clawdbot_1_clawdb_key_02.png]]
    - Keyframe 3 (42.00s): ![[vl_tutorial_units/SU012/SU012_step_03_clawdbot_gmail_api_gmail_api_7_clawdbot_1_clawdb_key_03.png]]
    - Step video: ![[vl_tutorial_units/SU012/SU012_step_03_clawdbot_gmail_api_gmail_api_7_clawdbot_1_clawdb.mp4]]

4. 4. 在WhatsApp中设置Clawdbot每2分钟检查一次Gmail未读邮件，若有新邮件则通过WhatsApp通知；随后从Gmail发送测试邮件，Clawdbot检测到后立即推送通知并总结邮件内容（含版本号n8n 2.6.0及Bug Fixes列表）。: from 621.57s to 631.57s
    - Keyframe 1 (52.50s): ![[vl_tutorial_units/SU012/SU012_step_04_whatsapp_clawdbot_2_gmail_whatsapp_gmail_clawdbo_key.png]]
    - Step video: ![[vl_tutorial_units/SU012/SU012_step_04_whatsapp_clawdbot_2_gmail_whatsapp_gmail_clawdbo.mp4]]


#### 配置MCP对接百度地图

主题: 配置MCP对接百度地图
	背景: AI助手对接其他生态的一个重要渠道是MCP
	步骤:
		- 安装MCP工具: 在skills中找到mcpporter并安装这个skill
		- 配置MCP: 告诉AI助手使用mcpporter来配置一个百度地图的MCP，并提供该MCP的说明文档
			- 选择安装方案: AI助手提供了三种安装方案，用户选择了DreamableHDDP
			- 提供API密钥: 用户需要提供百度地图的API-K，复制并粘贴给AI助手
		- 验证配置: 配置完成后，AI助手可以使用这个MCP查询地理位置的坐标
			- 示例查询: 用户让AI助手查询从青岛太平角公园到崂山仰口景区的路线，AI助手成功提供了规划路线
		- 扩展使用: 用户也可以在手机中使用这个配置


#### 使用coding agent编程案例

skills使用案例: coding agent
	功能: 驱动AI编程工具进行编程
		工具示例: codex、cloud code、open code
	实施步骤:
		- 安装coding agent skills
		- 在Mac电脑上登录codex账户
		- 在手机上向AI发出指令: 使用codex创建一个贪吃蛇游戏
	结果: 程序被成功编写，通过cloud bolt驱动Codex完成开发

