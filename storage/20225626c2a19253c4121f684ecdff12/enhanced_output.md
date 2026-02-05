# video

## Cloudbot项目介绍

- **Cloudbot项目介绍**：
	- **项目背景与关注度**：
		- **现象描述**：近期，一个名为Cloudbot的开源项目引发了广泛关注，甚至在硅谷地区形成了热议。
	- **项目核心定义**：
		- **功能特性**：Cloudbot是一个能够7×24小时持续运行，并可通过聊天工具进行操控的AI智能体。
	- **本文的论述动机与内容**：
		- **动机（因果）**：鉴于许多用户对该项目抱有浓厚兴趣，且我们已具备Mac mini等测试环境。
		- **内容预告（总分）**：本文将探讨Cloudbot的具体使用方法及其实际运行效果。

> 🖼️ **关键帧**

![[screenshots/SU001_island0.png]]
![[screenshots/SU001_island1.png]]


#### 作者背景故事

		- **作者背景故事**：
			- **引入背景的动机**：在开始技术内容之前，我想先介绍作者的故事，因为其经历颇具传奇色彩。
			- **作者身份澄清**：
				- **否定预设**：作者是Pietro Deberger。他并非人们可能以为的、试图挤入AI创业领域的投机者。
				- **真实情况**：实际上，Pietro Deberger早已实现了财务自由。
					- **证据**：他在15年前开始创业，并于2021年退出公司、套现离场，获得了约一亿欧元的收益。
			- **人生选择与转变**：
				- **常规预期**：拥有如此巨额的财富，许多人或许会选择享受奢华的生活。
				- **作者的实际选择**：但Pietro Deberger在仅仅过了四年闲适的日子后，便感到这种缺乏目标的生活令人空虚。
					- **结果**：于是，他决定回头，开始全职从事编程工作。
			- **引发的思考**：这一转变令人不禁思考：
				- **疑问一**：编程工作是否也具有如此强烈的吸引力？
				- **疑问二**：还是说，这体现了一种对充实与创造的内在追求？

> 🖼️ **关键帧**

![[screenshots/SU002_island0.png]]
![[screenshots/SU002_island1.png]]
![[screenshots/SU002_island2.png]]
![[screenshots/SU002_island3.png]]
![[screenshots/SU002_island4.png]]


#### Cloudbot功能与成就

		- **Cloudbot功能与成就**：
			- **项目概述**：在不到一年的时间里，该团队便成功开发出了Cloudbot——一款可运行于用户本地设备上的个人AI助手。
			- **核心功能**：
				- **调用方式**：用户能够通过熟悉的通讯工具（例如WhatsApp、Telegram或iMessage）来调用Cloudbot。
			- **项目成就**：
				- **市场反响**：该项目上线不到一个月，便在GitHub上获得了超过40,000个Star。
				- **发展趋势**：目前其关注度仍在快速增长。
				- **社区评价**：整个AI社区都对此表现出了极高的热情。

> 🖼️ **关键帧**

![[screenshots/SU003_island0.png]]
![[screenshots/SU003_island1.png]]
![[screenshots/SU003_island2.png]]
![[screenshots/SU003_island3.png]]
![[screenshots/SU003_island4.png]]


## 项目安装平台说明

- **项目安装平台说明**：
	- **总述安装特点**：该项目的安装过程较为简便。
	- **平台兼容性说明**：
		- **官方推荐环境**：官方文档推荐在 macOS 环境下运行。
		- **实际兼容环境**：
			- **对比关系**：用户并非必须使用 Mac 设备。
			- **并列关系**：
				- 也可以在 Linux 系统安装。
				- 或通过 Windows 的 WSL 进行安装。
				- **验证关系（举例）**：亦有成功在树莓派等设备上安装并运行的案例。
	- **兼容性原因分析**：
		- **因果关系**：这并不意外，因为若不在本地运行大型模型，该工具本身对计算资源的需求并不高。
	- **本文讲解环境选择**：
		- **递进关系**：本文将以 macOS 环境为例进行说明。

> 📹 **过程演示**

![[clips/clip_SU004_action0.mp4]]

> 🖼️ **关键帧**

![[screenshots/SU004_island0.png]]
![[screenshots/SU004_island1.png]]
![[screenshots/SU004_island2.png]]
![[screenshots/SU004_island3.png]]
![[screenshots/SU004_island4.png]]


### 一键安装脚本使用

	- **一键安装脚本使用**：
		- **推荐使用**：推荐使用一键安装脚本
			- **原因**：该脚本会自动下载所需的安装文件
		- **执行流程**：
			- **步骤一**：执行脚本
				- **后续状态**：请稍作等待
			- **步骤二**：系统将自动启动onboard配置流程

> 📹 **过程演示**

![[clips/clip_SU005_action0.mp4]]

> 🖼️ **关键帧**

![[screenshots/SU005_island0.png]]
![[screenshots/SU005_island1.png]]
![[screenshots/SU005_island2.png]]


### 配置大模型供应商

	- **配置大模型供应商**：
		- **初始配置模式推荐**：
			- **模式选择**：首次使用推荐选择“Quickstart”模式。
			- **模式特点**：
				- **兼容性**：支持多个主流大模型供应商。
				- **用户友好性**：对国内用户较为友好。
				- **供应商示例**：常见的如千问、Minimax、智谱、Kimi等均已涵盖。
		- **个人配置实例**：
			- **选择依据**：由于我拥有Codex订阅。
			- **供应商选择**：因此选择OpenAI作为供应商。
			- **具体配置方式**：并具体采用了Codex CLI的OAuth方式进行配置。

> 📹 **过程演示**

![[clips/clip_SU006_action0.mp4]]

> 🖼️ **关键帧**

![[screenshots/SU006_island0.png]]
![[screenshots/SU006_island1.png]]
![[screenshots/SU006_island2.png]]
![[screenshots/SU006_island3.png]]
![[screenshots/SU006_island4.png]]
![[screenshots/SU006_island5.png]]
![[screenshots/SU006_island6.png]]
![[screenshots/SU006_island7.png]]


### 选择聊天工具Channel

	- **选择聊天工具Channel**：
		- **总述**：接下来选择通信渠道，通过聊天工具使用Cloudbot。
		- **具体选择与原因**：
			- **示例选择**：例如，选择iMessage。
			- **选择依据（因果）**：因为对于国内普通用户而言，其他工具的操作更为繁琐。
		- **后续步骤**：
			- **验证**：完成选择后，直接进行验证即可。
			- **补充配置（递进）**：选择iMessage之后，还需补充一些配置。
				- **说明安排**：这部分内容将在稍后详细说明。

> 📹 **过程演示**

![[clips/clip_SU007_action0.mp4]]

> 🖼️ **关键帧**

![[screenshots/SU007_island0.png]]
![[screenshots/SU007_island1.png]]
![[screenshots/SU007_island2.png]]
![[screenshots/SU007_island3.png]]
![[screenshots/SU007_island4.png]]


### 选择安装常用工具

	- **选择安装常用工具**：
		- **系统预设**：在安装过程中，系统通常会提供一些常用的工具供用户选择和安装。
		- **用户决策**：用户可以根据需要，从中选取若干工具进行安装。
		- **后续配置**：最后，系统会引导用户完成一些基本配置。
			- **配置建议**：
				- **保持默认**：建议保持默认选项。
				- **常规选择**：或根据常规需求进行选择。

> 🖼️ **关键帧**

![[screenshots/SU008_island0.png]]
![[screenshots/SU008_island1.png]]


### 选择使用方式与命名

	- **选择使用方式与命名**：
		- **步骤一：选择使用方式**：
			- **具体操作**：应选择默认的CLI方式。
		- **步骤二：进行系统命名**：
			- **触发时机**：首次进入系统时。
			- **系统行为**：提示用户设置名称并输入称呼。
			- **类比解释**：类似于游戏中的角色命名环节。
				- **用户自定义部分**：
					- **用户称呼**：用户可定义系统对自己的称呼，例如“馆长大人”。
					- **系统名称**：用户可为系统赋予拟人化名称，例如“小鸡”。
		- **步骤三：最终确认**：
			- **确认操作**：完成命名后，再次确认选择默认的CLI方式作为使用方式。

> 📹 **过程演示**

![[clips/clip_SU009_action0.mp4]]

> 🖼️ **关键帧**

![[screenshots/SU009_island0.png]]
![[screenshots/SU009_island1.png]]
![[screenshots/SU009_island2.png]]
![[screenshots/SU009_island3.png]]
![[screenshots/SU009_island4.png]]
![[screenshots/SU009_island5.png]]


#### 命名比喻介绍

		- **命名比喻介绍**：
			- **比喻本体**：用户与AI助手互动的过程
			- **比喻喻体**：角色扮演游戏
			- **具体类比展开**：
				- **用户扮演的角色**：馆长大人
					- **关键证据**：我对人工智能助手说：“你可以称呼我为馆长大人。”
				- **AI助手扮演的角色**：小鸡
					- **关键证据**：既然对方是一个人工智能程序，我便称呼它为“小鸡。”
			- **隐性逻辑关系**：
				- **因果**：因为将互动过程比作“角色扮演游戏”（总喻体），所以后续可以分别赋予双方具体的游戏角色（分喻体）。
				- **并列/对比**：用户（馆长大人）与AI助手（小鸡）的角色命名形成并列且带有趣味性对比的关系，共同支撑“角色扮演”这一核心比喻。

> 🖼️ **关键帧**

![[screenshots/SU010_island0.png]]
![[screenshots/SU010_island1.png]]
![[screenshots/SU010_island2.png]]


#### 使用方式对比

		- **使用方式对比**：
			- **方式一：终端使用**：
				- **操作体验**：与 Cloud Shell 类似
				- **key_evidence**：用户可以直接在终端中使用该工具，其操作体验与 Cloud Shell 类似。
			- **方式二：Web界面使用**：
				- **功能集成**：
					- 对话式交互功能
					- 查看各类配置信息
				- **key_evidence**：该工具也提供了内置的 Web 界面，用户可以通过浏览器打开并使用。该界面中集成了对话式交互功能，并支持查看各类配置信息。
				- **适用对象**：不习惯使用命令行界面的用户
				- **key_evidence**：对于不习惯使用命令行界面的用户，可以选择这种方式进行操作。
			- **对比分析**：
				- **Web界面的局限性**：无法充分体现该工具的核心优势
				- **key_evidence**：但若仅使用 Web 界面，则无法充分体现该工具的核心优势。

> 🖼️ **关键帧**

![[screenshots/SU011_island0.png]]
![[screenshots/SU011_island1.png]]
![[screenshots/SU011_island2.png]]
![[screenshots/SU011_island3.png]]
![[screenshots/SU011_island4.png]]
![[screenshots/SU011_island5.png]]


#### iMessage配置步骤

		- **iMessage配置步骤**：
			- **前置条件**：要使用iMessage功能，需要先完成前置的iMessage配置。
			- **安装与配置方式**：
				- **方式A（手动安装）**：首先，需安装一个命令行工具，该工具可通过Brew进行安装。
				- **方式B（自动安装）**：用户也可以选择让Cloudbot自动完成安装与配置。
					- **后续动作**：Cloudbot会在安装完成后自动重启，操作较为便捷。
			- **后续配置与验证**：
				- **询问与提示**：接下来，可询问Cloudbot是否还需要其他配置。Cloudbot通常会提示一些需要用户手动完成的系统设置。
				- **具体系统设置步骤**：这些设置主要在Mac系统内进行。
					- 进入“隐私与安全”设置。
					- 开启“完全磁盘访问权限”。
					- **建议**：建议为Node和终端工具授予相应权限。
				- **验证步骤**：完成授权后，可让Cloudbot发送一条测试信息进行验证。
			- **注意事项**：
				- **首次执行提示**：请注意，首次执行相关操作时，系统可能会弹出权限请求窗口。
				- **关键动作**：请务必选择允许。

> 📹 **过程演示**

![[clips/clip_SU012_action0.mp4]]

> 🖼️ **关键帧**

![[screenshots/SU012_island0.png]]
![[screenshots/SU012_island1.png]]
![[screenshots/SU012_island2.png]]
![[screenshots/SU012_island3.png]]
![[screenshots/SU012_island4.png]]
![[screenshots/SU012_island5.png]]
![[screenshots/SU012_island6.png]]
![[screenshots/SU012_island7.png]]
![[screenshots/SU012_island8.png]]
![[screenshots/SU012_island9.png]]


#### iMessage账号提醒

		- **iMessage账号提醒**：
			- **配置完成确认**：至此，配置步骤已全部完成。
			- **补充经验提醒**：但需额外提醒一点个人经验。
				- **核心建议**：请勿使用同一个 iMessage 账号进行消息收发。
					- **原因**：因为若同一账号向自身发送消息，系统通常会默认忽略该消息。
				- **解决方案**：因此，建议为运行 Cloud 服务的机器单独配置一个专用的 iMessage 账号。

> 🖼️ **关键帧**

![[screenshots/SU013_island0.png]]
![[screenshots/SU013_island1.png]]
![[screenshots/SU013_island2.png]]
![[screenshots/SU013_island3.png]]
![[screenshots/SU013_island4.png]]


## 开发功能实现流程

- **开发功能实现流程**：
	- **总述**：通过两个实际应用场景进行说明
	- **分述一：以软件开发为例**：
		- **前提设定**：假设有一个待开发的项目
		- **核心操作**：直接向智能开发助手发送指令，要求其实现特定功能
			- **具体示例**：指示助手完成商品详情页的开发任务
		- **后续流程**：
			- **步骤一**：开发完成后，助手自动编写功能描述并提交代码合并请求
			- **步骤二**：该过程会调用本地的 Codex 或云端的 Cloud Code 服务进行开发操作
		- **最终效果**：开发者无需操作电脑，即可远程指导助手完成全部开发工作

> 📹 **过程演示**

![[clips/clip_SU014_action0.mp4]]

> 🖼️ **关键帧**

![[screenshots/SU014_island0.png]]
![[screenshots/SU014_island1.png]]
![[screenshots/SU014_island2.png]]
![[screenshots/SU014_island3.png]]
![[screenshots/SU014_island4.png]]
![[screenshots/SU014_island5.png]]
![[screenshots/SU014_island6.png]]
![[screenshots/SU014_island7.png]]


### Issue处理流程

	- **Issue处理流程**：
		- **核心操作**：让系统检查是否有新的Issue提交
		- **条件判断**：若该Issue属于Bug类型
			- **执行动作**：进行相应的修复

> 🖼️ **关键帧**

![[screenshots/SU015_island0.png]]


#### AI代码责任提醒

		- **AI代码责任提醒**：
			- **核心观点**：在实际项目开发中，对于由AI生成的代码仍需谨慎使用。
			- **逻辑支撑**：
				- **因果**：尽管代码由AI辅助编写。
				- **对比**：但最终的责任仍需由开发者自行承担。

> 🖼️ **关键帧**

![[screenshots/SU016_island0.png]]
![[screenshots/SU016_island1.png]]


#### Cloudbot远程工作示例

		- **Cloudbot远程工作示例**：
			- **核心场景**：无需打开电脑，通过语音指令在床上指挥电脑工作
			- **具体指令示例**：
				- **指令1**：命令电脑查看是否有新的Issue
					- **条件判断**：若该Issue是Bug
						- **执行动作**：指示电脑自动修复它

> 🖼️ **关键帧**

![[screenshots/SU017_island0.png]]
![[screenshots/SU017_island1.png]]


#### Cloudbot语音任务处理示例

		- **Cloudbot语音任务处理示例**：
			- **总述**：一个日常工作中的应用示例
			- **分述处理流程**：
				- **步骤一：指令输入与智能处理**：
					- **动作**：给下级发送了一段关于年会安排的语音指令
					- **结果**：智能处理系统会自动将语音内容进行总结与任务拆分，生成可直接复用的文本
				- **步骤二：任务分发要求**：
					- **递进**：在此基础上，我进一步要求该系统将拆分后的具体任务发送至对应负责人的邮箱
				- **步骤三：系统执行与结果验证**：
					- **因果**：系统会自动执行相关操作
					- **验证**：完成后，我只需登录邮箱即可查看到任务邮件已全部发送完毕

> 🖼️ **关键帧**

![[screenshots/SU018_island0.png]]
![[screenshots/SU018_island1.png]]
![[screenshots/SU018_island2.png]]
![[screenshots/SU018_island3.png]]
![[screenshots/SU018_island4.png]]
![[screenshots/SU018_island5.png]]
![[screenshots/SU018_island6.png]]
![[screenshots/SU018_island7.png]]


#### 配置邮箱SMTP的提醒

		- **配置邮箱SMTP的提醒**：
			- **前提条件**：若需发送邮件，您需要预先配置邮箱的SMTP服务。
			- **内容说明**：本文省略了具体的配置步骤。
			- **解决方案**：如您不熟悉配置方法，可咨询Cloudbot。
				- **方案效果**：它将为您提供详细的配置指引。
				- **执行要求**：您只需按其要求操作即可。

> 🖼️ **关键帧**

![[screenshots/SU019_island0.png]]
![[screenshots/SU019_island1.png]]
![[screenshots/SU019_island2.png]]
![[screenshots/SU019_island3.png]]


## Cloudbot能力与使用建议

- **Cloudbot能力与使用建议**：
	- **现状评估与功能定位**：
		- **询问体验**：您感觉Cloudbot的效果如何？
		- **功能范围说明**：目前展示的仅是Cloudbot一小部分的功能。
	- **使用指导与资源指引**：
		- **问题导向建议**：若不清楚在何种场景中使用Cloudbot，建议查阅其UseCase板块。
		- **资源内容与价值**：
			- **内容构成**：该板块汇集了社区成员分享的使用心得。
			- **参考价值**：可供您参考并寻找灵感。

> 🖼️ **关键帧**

![[screenshots/SU020_island0.png]]
![[screenshots/SU020_island1.png]]


### Cloudbot本地模型配置建议

	- **Cloudbot本地模型配置建议**：
		- **特性说明**：Cloudbot支持使用本地模型。
		- **官方推荐方案**：使用LM Studio配合MiniMax 2.1模型。
			- **存在问题**：MiniMax 2.1模型体积较大，最小版本也无法在本地设备上顺利运行。
				- **具体表现**：无法在我的本地设备上顺利运行。
		- **实际采纳方案**：更换为最新的JRM 4.7模型。
			- **选择依据**：由于官方推荐方案存在问题（模型体积大导致无法运行）。

> 🖼️ **关键帧**

![[screenshots/SU021_island0.png]]


#### 本地模型运行与学习指引

		- **本地模型运行与学习指引**：
			- **模型部署与运行**：
				- **操作性质**：过程直接、常规
					- **具体操作**：下载模型文件并执行命令
				- **潜在问题与解决方案**：
					- **问题**：操作中遇到困难
					- **解决方案**：回顾作者发布的相关教程视频
						- **支撑依据**：关于在本地计算机上运行模型的详细步骤，我已进行过多次介绍。

> 🖼️ **关键帧**

![[screenshots/SU022_island0.png]]
![[screenshots/SU022_island1.png]]


#### Cloudbot本地模型配置方法

		- **Cloudbot本地模型配置方法**：
			- **部署后配置**：部署本地模型后，您可以直接在 Cloudbot 中进行配置。
			- **部署后使用**：部署本地模型后，您可以通过 Cloudbot 向该模型发送指令。

> 🖼️ **关键帧**

![[screenshots/SU023_island0.png]]
![[screenshots/SU023_island1.png]]


## Cloudbot架构解析引入

- **Cloudbot架构解析引入**：
	- **背景回顾**：在了解了Cloudbot的安装、配置与基本使用方法之后
	- **内容推进**：我们将进一步深入解析其系统架构
	- **总体评价**：Cloudbot最令人印象深刻之处，在于其完整而精良的架构设计

> 🖼️ **关键帧**

![[screenshots/SU024_island0.png]]


### Cloudbot架构图解析

	- **Cloudbot架构图解析**：
		- **总体架构概览**：
			- **接入层**：最外层的各种接入方式，例如不同的渠道（Channel）。
			- **核心中枢**：所有接入都会连接到Cloudbot的网关（Gateway）。
		- **网关核心功能**：
			- **统一管理**：负责统一管理会话。
			- **路由分发**：负责路由不同渠道。
			- **逻辑处理**：负责处理服务长期运行的逻辑。

> 🖼️ **关键帧**

![[screenshots/SU025_island0.png]]
![[screenshots/SU025_island1.png]]
![[screenshots/SU025_island2.png]]


### Agent Runtime功能与扩展模块

	- **Agent Runtime功能与扩展模块**：
		- **核心功能**：调用用户配置的大语言模型来生成响应、规划任务并执行具体操作。
		- **功能实现基础**：为了使智能体能够完成实际任务，Cloudbot配置了可插拔的Tools和Skills。
			- **扩展模块构成**：
				- **Tools**：封装了文件操作、浏览器控制、定时任务等能力，使智能体能够执行具体操作。
				- **Skills**：封装了文件操作、浏览器控制、定时任务等能力，使智能体能够执行具体操作。
			- **扩展模块的高级特性**：此外，这些模块还可以接入不同节点，实现在多个节点上完成各类任务。

> 🖼️ **关键帧**

![[screenshots/SU026_island0.png]]
![[screenshots/SU026_island1.png]]
![[screenshots/SU026_island2.png]]
![[screenshots/SU026_island3.png]]


#### 长期记忆机制原理

		- **长期记忆机制原理**：
			- **功能定位**：作为最底层的记忆机制
			- **实现方式**：
				- **数据载体**：通过本地存储的Markdown等格式文件
				- **核心过程**：积累用户的偏好与历史上下文信息
			- **实现目标**：
				- **核心功能**：实现跨对话的连续感知
				- **隐性逻辑（因果）**：通过积累历史信息，从而支撑连续感知能力的实现

> 🖼️ **关键帧**

![[screenshots/SU027_island0.png]]


## Cloudbot项目评价与潜力

- **Cloudbot项目评价与潜力**：
	- **总体评价**：作为一个开源项目，该项目的完成度已经非常高。
		- **对比**：尽管目前仍存在一些不完善之处，但我们确实无法要求一个开源项目在短短一个月内就全面超越成熟的商业产品。
	- **未来展望**：此外，我认为该项目还具有巨大的潜力，值得进一步挖掘。

> 🖼️ **关键帧**

![[screenshots/SU028_island0.png]]
![[screenshots/SU028_island1.png]]
![[screenshots/SU028_island2.png]]
![[screenshots/SU028_island3.png]]
![[screenshots/SU028_island4.png]]


### Cloudbot服务与接入方式特点

	- **Cloudbot服务与接入方式特点**：
		- **特点总述**：Cloudbot 有以下几个值得关注的特点
		- **特点分述**：
			- **强调服务化与自动化**：
				- **核心设计**：以“服务”为核心设计理念
				- **实现方式**：
					- **时间覆盖**：借助 AI 实现 24 小时不间断运行
					- **任务处理**：通过底层的工具与技能模块在后台自动处理各类任务
			- **接入方式友好便捷**：
				- **交互界面**：采用聊天工具式的交互界面
				- **效果**：
					- **降低门槛**：大幅降低了使用门槛
					- **提升便利**：提升了操作的便利性
			- **具备长期记忆与主动规划能力**：
				- **能力基础**：系统通过长期记忆机制持续学习用户的偏好与习惯
				- **能力应用**：
					- **主动规划**：结合定时任务功能，能够主动生成周期性的服务计划
				- **拓展潜力**：该平台仍具备进一步挖掘与拓展的潜力

> 🖼️ **关键帧**

![[screenshots/SU029_island0.png]]
![[screenshots/SU029_island1.png]]
![[screenshots/SU029_island2.png]]
![[screenshots/SU029_island3.png]]
![[screenshots/SU029_island4.png]]


#### 节目结束语

		- **节目结束语**：
			- **总结收束**：这里是IT咖啡馆
			- **预告延续**：我们下次再见

> 🖼️ **关键帧**

![[screenshots/SU030_island0.png]]

