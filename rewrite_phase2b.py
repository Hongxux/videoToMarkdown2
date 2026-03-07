import re

file_path = "d:/videoToMarkdownTest2/services/java-orchestrator/src/main/resources/static/lib/mobile-anchor-panel.js"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

css_new = """            #anchorMountPanel {
                --p2b-bg: rgba(255, 255, 255, 0.65);
                --p2b-border: rgba(0, 0, 0, 0.08);
                --p2b-border-inner: rgba(255, 255, 255, 0.5);
                --p2b-text-primary: #1e293b;
                --p2b-text-secondary: #64748b;
                --p2b-text-muted: #94a3b8;
                --p2b-input-bg: rgba(0, 0, 0, 0.03);
                --p2b-input-bg-focus: rgba(255, 255, 255, 0.8);
                --p2b-input-border: transparent;
                --p2b-accent: #2563eb;
                --p2b-accent-hover: #1d4ed8;
                --p2b-shadow: rgba(15, 23, 42, 0.16);
                --p2b-shadow-focus: rgba(37, 99, 235, 0.12);
                --p2b-btn-ghost-bg: transparent;
                --p2b-btn-ghost-hover: rgba(0, 0, 0, 0.06);
                --p2b-capsule-bg: rgba(255, 255, 255, 0.74);
            }
            @media (prefers-color-scheme: dark) {
                #anchorMountPanel {
                    --p2b-bg: rgba(30, 30, 30, 0.65);
                    --p2b-border: rgba(255, 255, 255, 0.15);
                    --p2b-border-inner: rgba(255, 255, 255, 0.05);
                    --p2b-text-primary: #f8fafc;
                    --p2b-text-secondary: #94a3b8;
                    --p2b-text-muted: #64748b;
                    --p2b-input-bg: rgba(255, 255, 255, 0.05);
                    --p2b-input-bg-focus: rgba(255, 255, 255, 0.08);
                    --p2b-accent: #3b82f6;
                    --p2b-accent-hover: #60a5fa;
                    --p2b-shadow: rgba(0, 0, 0, 0.4);
                    --p2b-shadow-focus: rgba(59, 130, 246, 0.2);
                    --p2b-btn-ghost-hover: rgba(255, 255, 255, 0.1);
                    --p2b-capsule-bg: rgba(40, 40, 40, 0.74);
                }
            }
            #anchorMountPanel .anchor-phase2b-dock { position: absolute; right: clamp(10px, 2vw, 24px); bottom: clamp(10px, 2vh, 24px); z-index: 42; display: flex; flex-direction: column; align-items: flex-end; pointer-events: none; max-width: calc(100% - 10px); font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; letter-spacing: -0.01em; }
            #anchorMountPanel .anchor-phase2b-dock > * { pointer-events: auto; }
            #anchorMountPanel .anchor-phase2b-capsule { width: 44px; height: 44px; border-radius: 999px; border: 1px solid var(--p2b-border); background: var(--p2b-capsule-bg); color: var(--p2b-text-primary); backdrop-filter: blur(28px) saturate(180%); -webkit-backdrop-filter: blur(28px) saturate(180%); box-shadow: inset 0 1px 0 var(--p2b-border-inner), 0 12px 30px var(--p2b-shadow); display: inline-flex; align-items: center; justify-content: center; cursor: grab; transition: transform .34s cubic-bezier(0.16, 1, 0.3, 1), box-shadow .34s cubic-bezier(0.16, 1, 0.3, 1), opacity .2s ease; touch-action: none; user-select: none; }
            #anchorMountPanel .anchor-phase2b-capsule:hover { transform: translateY(-2px) scale(1.04); box-shadow: inset 0 1px 0 var(--p2b-border-inner), 0 16px 36px var(--p2b-shadow); }
            #anchorMountPanel .anchor-phase2b-dock.is-moving .anchor-phase2b-capsule { cursor: grabbing; box-shadow: inset 0 1px 0 var(--p2b-border-inner), 0 8px 20px var(--p2b-shadow); opacity: 0.9; }
            #anchorMountPanel .anchor-phase2b-capsule-icon { font-size: 16px; color: var(--p2b-text-primary); line-height: 1; opacity: 0.8; }
            #anchorMountPanel .anchor-phase2b-capsule-label { display: none !important; }
            #anchorMountPanel .anchor-phase2b-capsule-indicator { width: 8px; height: 8px; border-radius: 999px; background: transparent; transition: all .2s ease; position: absolute; right: 8px; top: 8px; pointer-events: none; }
            #anchorMountPanel .anchor-phase2b-dock.is-processing .anchor-phase2b-capsule-indicator { background: #10b981; animation: anchorPhase2bPulse 1.5s cubic-bezier(0.4, 0, 0.6, 1) infinite; }
            #anchorMountPanel .anchor-phase2b-dock.is-ready:not(.is-processing) .anchor-phase2b-capsule-indicator { background: var(--p2b-accent); box-shadow: 0 0 0 4px rgba(59,130,246,.15); }
            #anchorMountPanel .anchor-phase2b-toast { margin-top: 8px; padding: 8px 14px; border-radius: 10px; background: rgba(15,23,42,.88); color: #fff; font-size: 12px; line-height: 1.4; opacity: 0; transform: translateY(6px) scale(.98); transition: opacity .25s ease, transform .25s ease; pointer-events: none; max-width: min(260px, 72vw); box-shadow: 0 12px 30px rgba(0,0,0,.24); text-align: center; }
            #anchorMountPanel .anchor-phase2b-dock.is-notice .anchor-phase2b-toast { opacity: 1; transform: translateY(0) scale(1); }
            #anchorMountPanel .anchor-phase2b-dock.is-notice .anchor-phase2b-capsule { animation: anchorPhase2bNotify .36s cubic-bezier(0.16, 1, 0.3, 1) 1; }
            
            #anchorMountPanel .anchor-phase2b-canvas { width: min(460px, calc(100vw - 56px), calc(100% - 8px)); max-height: min(74vh, 760px, calc(100% - 64px)); border-radius: 18px; border: 1px solid var(--p2b-border); background: var(--p2b-bg); backdrop-filter: blur(40px) saturate(150%); -webkit-backdrop-filter: blur(40px) saturate(150%); box-shadow: inset 0 1px 0 var(--p2b-border-inner), 0 24px 60px var(--p2b-shadow); padding: 16px 20px 20px 20px; display: grid; gap: 12px; grid-template-rows: auto auto auto minmax(0,1fr) auto; transform-origin: right bottom; transform: translateY(16px) scale(.92); opacity: 0; pointer-events: none; transition: transform .4s cubic-bezier(0.16, 1, 0.3, 1), opacity .3s ease; position: relative; min-height: 280px; }
            #anchorMountPanel .anchor-phase2b-dock.is-open .anchor-phase2b-canvas { transform: translateY(0) scale(1); opacity: 1; pointer-events: auto; }
            #anchorMountPanel .anchor-phase2b-dock.is-moving .anchor-phase2b-canvas { transition: none; }
            #anchorMountPanel .anchor-phase2b-dock.is-open .anchor-phase2b-capsule { opacity: 0; transform: translateY(12px) scale(.8); pointer-events: none; }
            
            #anchorMountPanel .anchor-phase2b-canvas-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; cursor: move; user-select: none; touch-action: none; padding-bottom: 4px; }
            #anchorMountPanel .anchor-phase2b-canvas-title { font-size: 14px; font-weight: 600; color: var(--p2b-text-primary); }
            #anchorMountPanel .anchor-phase2b-canvas-actions { display: inline-flex; align-items: center; gap: 4px; }
            #anchorMountPanel .anchor-phase2b-canvas-actions .btn { min-width: 28px; min-height: 28px; border-radius: 8px; padding: 0 6px; font-size: 14px; cursor: pointer; color: var(--p2b-text-secondary); background: transparent; transition: background .2s ease, color .2s ease; border: none; outline: none;}
            #anchorMountPanel .anchor-phase2b-canvas-actions .btn:hover { background: var(--p2b-btn-ghost-hover); color: var(--p2b-text-primary); }
            
            #anchorMountPanel .anchor-phase2b-chips { display: flex; align-items: center; flex-wrap: wrap; gap: 8px; }
            #anchorMountPanel .anchor-phase2b-chip { border: 1px solid rgba(148,163,184,.2); background: rgba(148,163,184,.12); color: var(--p2b-text-secondary); border-radius: 999px; padding: 3px 10px; font-size: 11px; font-weight: 500; line-height: 1.5; max-width: 100%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
            #anchorMountPanel .anchor-phase2b-chip.is-link { border-color: rgba(59,130,246,.2); background: rgba(59,130,246,.08); color: var(--p2b-accent); max-width: min(100%, 360px); }
            #anchorMountPanel .anchor-phase2b-chip-site { width: 14px; height: 14px; border-radius: 999px; background: rgba(59,130,246,.2); color: var(--p2b-accent); font-size: 9px; font-weight: 700; display: inline-flex; align-items: center; justify-content: center; flex: 0 0 auto; margin-right: 6px; }
            
            #anchorMountPanel .anchor-phase2b-input-wrap { display: grid; gap: 8px; transition: opacity .4s ease, margin .4s cubic-bezier(0.16, 1, 0.3, 1); }
            #anchorMountPanel .anchor-phase2b-input-wrap.is-collapsed { display: none; }
            #anchorMountPanel .anchor-phase2b-dock.is-processing .anchor-phase2b-input-wrap { opacity: 0.3; pointer-events: none; margin-bottom: -20px; transform: scale(0.96); transform-origin: top center; transition: all .4s cubic-bezier(0.16, 1, 0.3, 1); }
            
            #anchorMountPanel .anchor-phase2b-input-shell { position: relative; border-radius: 14px; background: var(--p2b-input-bg); transition: background .3s ease, box-shadow .3s ease; box-shadow: 0 2px 8px rgba(0,0,0,0.02) inset; border: 1px solid transparent; }
            #anchorMountPanel .anchor-phase2b-input-shell:focus-within { background: var(--p2b-input-bg-focus); box-shadow: 0 0 0 2px var(--p2b-shadow-focus), 0 4px 12px rgba(0,0,0,0.04); }
            #anchorMountPanel .anchor-phase2b-dock.is-dragging .anchor-phase2b-input-shell { background: var(--p2b-input-bg-focus); box-shadow: 0 0 0 2px var(--p2b-accent), 0 14px 30px var(--p2b-shadow-focus); }
            
            #anchorMountPanel .anchor-phase2b-input { width: 100%; resize: none; border: 0; outline: none; background: transparent; color: var(--p2b-text-primary); padding: 14px 44px 14px 14px; font-size: 14px; line-height: 1.6; min-height: 96px; max-height: 38vh; overflow-y: auto; font-family: inherit; }
            #anchorMountPanel .anchor-phase2b-input::placeholder { color: var(--p2b-text-muted); }
            #anchorMountPanel .anchor-phase2b-input::-webkit-scrollbar { width: 6px; }
            #anchorMountPanel .anchor-phase2b-input::-webkit-scrollbar-thumb { border-radius: 999px; background: rgba(148,163,184,.3); }
            
            #anchorMountPanel .anchor-phase2b-clear { position: absolute; right: 8px; top: 8px; width: 24px; height: 24px; border-radius: 50%; background: rgba(148,163,184,.2); color: var(--p2b-text-secondary); border: none; font-size: 14px; display: inline-flex; align-items: center; justify-content: center; cursor: pointer; transition: background .2s, color .2s; }
            #anchorMountPanel .anchor-phase2b-clear:hover { background: rgba(148,163,184,.4); color: var(--p2b-text-primary); }
            
            #anchorMountPanel .anchor-phase2b-submit { position: absolute; right: 8px; bottom: 8px; width: 32px; height: 32px; border-radius: 999px; border: none; background: var(--p2b-text-secondary); color: #fff; font-size: 16px; font-weight: bold; cursor: pointer; transition: transform .2s cubic-bezier(0.16, 1, 0.3, 1), background .3s, opacity .3s; display: inline-flex; align-items: center; justify-content: center; outline: none; opacity: 0.6; }
            #anchorMountPanel .anchor-phase2b-input-shell:focus-within .anchor-phase2b-submit, #anchorMountPanel .anchor-phase2b-submit.is-active { background: var(--p2b-accent); opacity: 1; box-shadow: 0 4px 12px var(--p2b-shadow-focus); }
            #anchorMountPanel .anchor-phase2b-submit:hover:not(:disabled) { transform: scale(1.08); background: var(--p2b-accent-hover); }
            #anchorMountPanel .anchor-phase2b-submit:active:not(:disabled) { transform: scale(0.95); }
            #anchorMountPanel .anchor-phase2b-submit:disabled { opacity: .4; cursor: not-allowed; background: var(--p2b-text-secondary); box-shadow: none; transform: none; }
            
            #anchorMountPanel .anchor-phase2b-processing { border-radius: 12px; min-height: 100px; padding: 16px; position: relative; overflow: hidden; background: transparent; display: grid; align-content: center; justify-content: center; gap: 12px; text-align: center; opacity: 0; pointer-events: none; transition: opacity .3s ease; transform: translateY(-10px); }
            #anchorMountPanel .anchor-phase2b-dock.is-processing .anchor-phase2b-processing { opacity: 1; pointer-events: auto; transform: translateY(0); }
            #anchorMountPanel .anchor-phase2b-processing-text { font-size: 13px; color: var(--p2b-text-secondary); font-weight: 500; line-height: 1.6; animation: anchorPhase2bFadePulse 2s ease-in-out infinite; }
            
            #anchorMountPanel .anchor-phase2b-result { display: grid; gap: 12px; min-height: 0; align-content: start; animation: anchorPhase2bSlideUp .4s cubic-bezier(0.16, 1, 0.3, 1); }
            #anchorMountPanel .anchor-phase2b-result-head { display: flex; align-items: center; justify-content: center; margin-top: 4px; padding-bottom: 4px; }
            #anchorMountPanel .anchor-phase2b-copy-btn { min-width: 140px; min-height: 38px; padding: 0 20px; border-radius: 999px; border: none; background: var(--p2b-text-primary); color: var(--p2b-bg); font-size: 13px; font-weight: 600; cursor: pointer; transition: all .25s cubic-bezier(0.16, 1, 0.3, 1); box-shadow: 0 8px 16px var(--p2b-shadow-focus); letter-spacing: 0.02em; display: inline-flex; align-items: center; justify-content: center; gap: 6px; }
            #anchorMountPanel .anchor-phase2b-copy-btn:hover:not(:disabled) { transform: translateY(-1px); box-shadow: 0 12px 24px var(--p2b-shadow-focus); opacity: 0.9; }
            #anchorMountPanel .anchor-phase2b-copy-btn:active:not(:disabled) { transform: scale(0.96); box-shadow: 0 4px 8px var(--p2b-shadow-focus); }
            #anchorMountPanel .anchor-phase2b-copy-btn.is-copied { background: #10b981; color: #fff; box-shadow: 0 8px 16px rgba(16,185,129,.2); }
            #anchorMountPanel .anchor-phase2b-copy-btn:disabled { opacity: .5; cursor: not-allowed; box-shadow: none; }
            
            #anchorMountPanel .anchor-phase2b-preview { max-height: min(40vh, 360px); overflow: auto; border-radius: 12px; border: 1px solid var(--p2b-border); background: var(--p2b-input-bg); padding: 14px 16px; font-size: 13px; line-height: 1.6; color: var(--p2b-text-primary); white-space: pre-wrap; word-break: break-word; }
            #anchorMountPanel .anchor-phase2b-preview :is(p,ul,ol,blockquote,pre,table,h1,h2,h3,h4,h5,h6) { margin: 0 0 .8em; }
            #anchorMountPanel .anchor-phase2b-preview p { margin-block: .6em; }
            #anchorMountPanel .anchor-phase2b-preview :is(p,li,blockquote,td,th) { white-space: pre-wrap; }
            #anchorMountPanel .anchor-phase2b-preview code { background: rgba(148,163,184,.15); padding: .1em .4em; border-radius: 4px; font-family: monospace; font-size: 0.9em; }
            #anchorMountPanel .anchor-phase2b-preview pre { background: rgba(15,23,42,.8); color: #e2e8f0; padding: 12px 14px; border-radius: 8px; overflow: auto; white-space: pre; border: 1px solid rgba(255,255,255,0.1); }
            #anchorMountPanel .anchor-phase2b-preview.is-streaming > * { animation: anchorPhase2bChunkIn .3s cubic-bezier(0.16, 1, 0.3, 1) both; }
            
            #anchorMountPanel .anchor-phase2b-feedback { font-size: 12px; line-height: 1.5; color: var(--p2b-text-secondary); min-height: 18px; text-align: center; }
            #anchorMountPanel .anchor-phase2b-feedback.is-error { color: #ef4444; }
            
            #anchorMountPanel .anchor-phase2b-resizer { position: absolute; width: 16px; height: 16px; right: 4px; bottom: 4px; cursor: nwse-resize; opacity: 0.3; transition: opacity .2s; touch-action: none; background: radial-gradient(circle at 70% 70%, var(--p2b-text-secondary) 15%, transparent 16%); background-size: 4px 4px; border-radius: 0 0 16px 0; }
            #anchorMountPanel .anchor-phase2b-canvas:hover .anchor-phase2b-resizer { opacity: 0.6; }
            #anchorMountPanel .anchor-phase2b-dock.is-resizing .anchor-phase2b-canvas { transition: none; }
            
            .viewer-layout.is-center-right-stacked #anchorMountPanel .anchor-phase2b-dock { right: 12px; bottom: 12px; }
            .viewer-layout.is-center-right-stacked #anchorMountPanel .anchor-phase2b-canvas { width: min(420px, calc(100% - 4px)); max-height: min(62vh, calc(100% - 52px)); }
            @media (max-width: 960px) { #anchorMountPanel .anchor-phase2b-dock { right: 12px; bottom: 12px; } #anchorMountPanel .anchor-phase2b-canvas { width: min(420px, calc(100vw - 28px), calc(100% - 4px)); } }
            
            @keyframes anchorPhase2bPulse { 0% { box-shadow: 0 0 0 0 rgba(16,185,129,.4); } 70% { box-shadow: 0 0 0 8px rgba(16,185,129,0); } 100% { box-shadow: 0 0 0 0 rgba(16,185,129,0); } }
            @keyframes anchorPhase2bNotify { 0% { transform: translateY(0) scale(1); } 35% { transform: translateY(-3px) scale(1.06); } 100% { transform: translateY(0) scale(1); } }
            @keyframes anchorPhase2bChunkIn { 0% { opacity: 0; transform: translateY(6px); } 100% { opacity: 1; transform: translateY(0); } }
            @keyframes anchorPhase2bFadePulse { 0% { opacity: 0.6; } 50% { opacity: 1; } 100% { opacity: 0.6; } }
            @keyframes anchorPhase2bSlideUp { 0% { opacity: 0; transform: translateY(10px); } 100% { opacity: 1; transform: translateY(0); } }"""

html_new = """                <button class="anchor-phase2b-capsule" id="anchorPhase2bCapsuleBtn" type="button" data-phase2b-action="open" aria-label="打开 Phase2B 结构化输入面板">
                    <span class="anchor-phase2b-capsule-icon" aria-hidden="true">✦</span>
                    <span class="anchor-phase2b-capsule-indicator" id="anchorPhase2bCapsuleIndicator" aria-hidden="true"></span>
                </button>
                <div class="anchor-phase2b-toast" id="anchorPhase2bNotice" hidden></div>
                <section class="anchor-phase2b-canvas" id="anchorPhase2bCanvas" hidden aria-hidden="true">
                    <div class="anchor-phase2b-canvas-head">
                        <div class="anchor-phase2b-canvas-title">Phase2B 提示词结构化</div>
                        <div class="anchor-phase2b-canvas-actions">
                            <button class="btn btn-ghost-icon" id="anchorPhase2bHeadSubmitBtn" type="button" data-phase2b-action="submit" title="重新发送">⟳</button>
                            <button class="btn btn-ghost-icon" id="anchorPhase2bHeadCopyBtn" type="button" data-phase2b-action="copy" title="一键复制">⧉</button>
                            <button class="btn btn-ghost-icon" id="anchorPhase2bToggleInputBtn" type="button" data-phase2b-action="toggle-input" title="收起输入">▾</button>
                            <button class="btn btn-ghost-icon" type="button" data-phase2b-action="collapse" title="最小化面板>×</button>
                        </div>
                    </div>
                    <div class="anchor-phase2b-chips" id="anchorPhase2bFileChips" hidden></div>
                    <div class="anchor-phase2b-input-wrap" id="anchorPhase2bInputWrap">
                        <div class="anchor-phase2b-input-shell" id="anchorPhase2bActionLayer">
                            <textarea id="anchorPhase2bInput" class="anchor-phase2b-input" placeholder="直接粘贴旧文本即被覆盖！" spellcheck="false"></textarea>
                            <button id="anchorPhase2bClearBtn" class="anchor-phase2b-clear" type="button" data-phase2b-action="clear" aria-label="清空内容" hidden title="清空内容">×</button>
                            <button id="anchorPhase2bSubmitBtn" class="anchor-phase2b-submit" type="button" data-phase2b-action="submit" aria-label="发送">↑</button>
                        </div>
                    </div>
                    <div class="anchor-phase2b-processing" id="anchorPhase2bProcessing"></div>
                    <div class="anchor-phase2b-result" id="anchorPhase2bResult" hidden>
                        <div class="anchor-phase2b-preview" id="anchorPhase2bResultPreview"></div>
                        <div class="anchor-phase2b-result-head">
                            <button id="anchorPhase2bCopyBtn" class="anchor-phase2b-copy-btn" type="button" data-phase2b-action="copy">
                                <span>⧉</span> 一键复制结果
                            </button>
                        </div>
                    </div>
                    <div class="anchor-phase2b-feedback" id="anchorPhase2bFeedback" hidden></div>
                    <div class="anchor-phase2b-resizer" id="anchorPhase2bResizer" aria-hidden="true"></div>
                </section>"""

content = re.sub(
    r'#anchorMountPanel \{ position: relative; \}.*?@keyframes anchorPhase2bChunkIn \{ 0% \{ opacity: \.16; transform: translateY\(4px\); \} 100% \{ opacity: 1; transform: translateY\(0\); \} \}',
    css_new,
    content,
    flags=re.DOTALL
)

content = re.sub(
    r'<button class="anchor-phase2b-capsule" id="anchorPhase2bCapsuleBtn" type="button" data-phase2b-action="open" aria-label="打开 Phase2B 结构化输入面板">.*?<div class="anchor-phase2b-resizer" id="anchorPhase2bResizer" aria-hidden="true"></div>\s*</section>',
    html_new,
    content,
    flags=re.DOTALL
)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("Replacement successful.")
