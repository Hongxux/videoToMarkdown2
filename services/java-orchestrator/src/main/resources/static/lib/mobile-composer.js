(function (global) {
    'use strict';

    function createMobileComposerController(options) {
        var opts = options || {};
        var composerCopy = opts.composerCopy || {};
        var uiCopy = opts.uiCopy || {};
        var userBusyMessage = String(opts.userBusyMessage || '系统繁忙，请稍后重试');
        var normalizeTaskVideoInput = typeof opts.normalizeTaskVideoInput === 'function'
            ? opts.normalizeTaskVideoInput
            : function (input) { return String(input || '').trim(); };
        var normalizeErrorMessage = typeof opts.normalizeErrorMessage === 'function'
            ? opts.normalizeErrorMessage
            : function (error) {
                return String((error && error.message) || error || userBusyMessage);
            };
        var submitTaskFromMobileForm = typeof opts.submitTaskFromMobileForm === 'function'
            ? opts.submitTaskFromMobileForm
            : function () { return Promise.resolve(false); };
        var onFileSelected = typeof opts.onFileSelected === 'function'
            ? opts.onFileSelected
            : null;

        var elementIds = Object.assign({
            surfaceId: 'mobileSourceSurface',
            inputId: 'mobileVideoUrl',
            fileInputId: 'mobileVideoFile',
            tipId: 'mobileSubmitTip',
            surfaceCopyId: 'mobileSourceCopy',
        }, opts.elements || {});

        var autoSubmitInFlight = false;
        var autoSubmitLastSignature = '';
        var autoSubmitLastAt = 0;
        var tipActionInFlight = false;

        function getElement(id) {
            return document.getElementById(String(id || ''));
        }

        function getInputNode() {
            return getElement(elementIds.inputId);
        }

        function getFileInputNode() {
            return getElement(elementIds.fileInputId);
        }

        // 做什么：统一读取提交区文案；为什么：避免页面脚本和组件脚本维护两套口径。
        // 取舍：优先读外部注入文案，缺失时才降级到调用方传入的 fallback。
        function copyText(key, fallback) {
            var value = composerCopy && composerCopy[key];
            if (typeof value === 'string' && value.trim()) {
                return value.trim();
            }
            return String(fallback || '');
        }

        function applyComposerCopy() {
            var inputNode = getInputNode();
            if (inputNode) {
                inputNode.setAttribute('placeholder', copyText('inputPlaceholder', '粘贴一个 B 站或抖音链接，我来帮你整理'));
            }
            var shellNode = getElement(elementIds.surfaceId);
            if (shellNode) {
                shellNode.setAttribute('aria-label', copyText('sourceAriaLabel', '素材入口，可粘贴 B 站或抖音链接并快速整理'));
            }
            var surfaceCopyNode = getElement(elementIds.surfaceCopyId);
            if (surfaceCopyNode) {
                surfaceCopyNode.textContent = copyText('surfacePrompt', '支持拖入 PDF/TXT/MD/EPUB，也可点击回形针上传');
            }
        }

        function setTaskSubmitTip(message, type, options) {
            var tipOptions = options || {};
            var normalizedType = String(type || '');
            var tipMessage = String(message || '').trim();
            var tipNode = getElement(elementIds.tipId);
            if (tipNode) {
                tipNode.textContent = '';
                tipNode.classList.remove('success', 'error', 'with-action', 'is-entering');
                if (normalizedType === 'success') {
                    tipNode.classList.add('success');
                } else if (normalizedType === 'error') {
                    tipNode.classList.add('error');
                }
                if (tipMessage) {
                    var contentNode = document.createElement('span');
                    contentNode.className = 'submit-tip-content';

                    var textNode = document.createElement('span');
                    textNode.className = 'submit-tip-text';
                    textNode.textContent = tipMessage;
                    contentNode.appendChild(textNode);

                    var actionHandler = typeof tipOptions.onAction === 'function'
                        ? tipOptions.onAction
                        : (typeof tipOptions.onRetry === 'function' ? tipOptions.onRetry : null);
                    if (actionHandler && normalizedType === 'error') {
                        var actionButton = document.createElement('button');
                        actionButton.type = 'button';
                        actionButton.className = 'submit-tip-action';
                        actionButton.textContent = String(tipOptions.actionLabel || tipOptions.retryLabel || '重试');
                        actionButton.addEventListener('click', function () {
                            if (tipActionInFlight) return;
                            tipActionInFlight = true;
                            actionButton.disabled = true;
                            actionButton.textContent = String(tipOptions.pendingLabel || '重试中...');
                            Promise.resolve()
                                .then(function () { return actionHandler(); })
                                .catch(function () { return null; })
                                .finally(function () {
                                    tipActionInFlight = false;
                                    actionButton.disabled = false;
                                    actionButton.textContent = String(tipOptions.actionLabel || tipOptions.retryLabel || '重试');
                                });
                        });
                        contentNode.appendChild(actionButton);
                        tipNode.classList.add('with-action');
                    }

                    tipNode.appendChild(contentNode);
                    if (typeof global.requestAnimationFrame === 'function') {
                        global.requestAnimationFrame(function () {
                            tipNode.classList.add('is-entering');
                        });
                    } else {
                        tipNode.classList.add('is-entering');
                    }
                }
            }
            var inputNode = getInputNode();
            var shouldHighlightLink = !!tipOptions.highlightLink;
            var surfaceNode = getElement(elementIds.surfaceId);
            if (surfaceNode && surfaceNode.classList) {
                surfaceNode.classList.toggle('is-link-suspect', shouldHighlightLink && normalizedType === 'error');
            }
            if (inputNode && inputNode.classList) {
                inputNode.classList.toggle('is-link-suspect', shouldHighlightLink && normalizedType === 'error');
            }
            if (!inputNode) return;
            var fallback = copyText('inputPlaceholder', '粘贴一个 B 站或抖音链接，我来帮你整理');
            var nextHint = tipMessage;
            if (!nextHint) {
                nextHint = fallback;
            } else if (normalizedType === 'error') {
                nextHint = copyText('errorTip', '这条素材还读不懂，换一个试试');
            }
            inputNode.setAttribute('placeholder', nextHint);
            if (shouldHighlightLink && normalizedType === 'error' && inputNode.value && typeof inputNode.setSelectionRange === 'function') {
                try {
                    inputNode.setSelectionRange(0, inputNode.value.length);
                } catch (_error) {
                    // ignore selection errors
                }
            }
        }

        function triggerComposerSurfacePulse(kind, clientX, clientY) {
            var surface = getElement(elementIds.surfaceId);
            if (!surface) return;
            var normalizedKind = String(kind || 'neutral');
            surface.classList.remove('is-emitting', 'is-success', 'is-error');
            if (normalizedKind === 'success') {
                surface.classList.add('is-success');
            } else if (normalizedKind === 'error') {
                surface.classList.add('is-error');
                surface.classList.remove('is-rejecting');
                void surface.offsetWidth;
                surface.classList.add('is-rejecting');
            } else {
                surface.classList.add('is-emitting');
            }

            var rect = surface.getBoundingClientRect();
            var ripple = document.createElement('span');
            var size = Math.max(rect.width, rect.height) * 1.28;
            var originX = Number.isFinite(clientX) ? clientX : (rect.left + rect.width - 40);
            var originY = Number.isFinite(clientY) ? clientY : (rect.top + rect.height / 2);
            ripple.className = 'submit-launch-ripple';
            ripple.style.width = size + 'px';
            ripple.style.height = size + 'px';
            ripple.style.left = (originX - rect.left - (size / 2)) + 'px';
            ripple.style.top = (originY - rect.top - (size / 2)) + 'px';
            surface.appendChild(ripple);
            ripple.addEventListener('animationend', function () { ripple.remove(); }, { once: true });
            global.setTimeout(function () { ripple.remove(); }, 900);

            global.setTimeout(function () {
                surface.classList.remove('is-emitting', 'is-success', 'is-error');
            }, 420);
            global.setTimeout(function () {
                surface.classList.remove('is-rejecting');
            }, 620);
        }

        function triggerComposerLaunchFx(kind) {
            var normalizedKind = String(kind || 'launch');
            var pulseKind = normalizedKind === 'launch' ? 'neutral' : normalizedKind;
            triggerComposerSurfacePulse(pulseKind);
        }

        async function triggerComposerAutoSubmit(source) {
            var urlInput = getInputNode();
            var fileInput = getFileInputNode();
            var rawInput = urlInput ? String(urlInput.value || '').trim() : '';
            var normalizedVideoUrl = normalizeTaskVideoInput(rawInput);
            var selectedVideoFile = fileInput && fileInput.files && fileInput.files.length > 0
                ? fileInput.files[0]
                : null;
            if (!selectedVideoFile && !normalizedVideoUrl) {
                return false;
            }
            var signature = selectedVideoFile
                ? ('file:' + selectedVideoFile.name + ':' + selectedVideoFile.size + ':' + (selectedVideoFile.lastModified || 0))
                : ('url:' + normalizedVideoUrl);
            var now = Date.now();
            if (autoSubmitInFlight) {
                return false;
            }
            if (signature === autoSubmitLastSignature && (now - autoSubmitLastAt) < 1400) {
                return false;
            }
            autoSubmitInFlight = true;
            autoSubmitLastSignature = signature;
            autoSubmitLastAt = now;
            try {
                await submitTaskFromMobileForm({ source: source, silentIfEmpty: true });
                return true;
            } finally {
                autoSubmitInFlight = false;
            }
        }

        function buildSubmitErrorFeedback(error, hasFile) {
            return (global.MobileSubmitFeedback && typeof global.MobileSubmitFeedback.classifySubmitError === 'function')
                ? global.MobileSubmitFeedback.classifySubmitError({
                    error: error,
                    hasFile: !!hasFile,
                }, {
                    normalizeErrorMessage: normalizeErrorMessage,
                    busyMessage: userBusyMessage,
                })
                : {
                    message: normalizeErrorMessage(error, { fallback: userBusyMessage }),
                    highlightLink: false,
                    allowRetry: true,
                };
        }

        function handleSubmitFailure(error, hasFile, retrySource) {
            var feedback = buildSubmitErrorFeedback(error, hasFile);
            setTaskSubmitTip(feedback.message, 'error', {
                highlightLink: !!feedback.highlightLink,
                onRetry: function () {
                    return submitTaskFromMobileForm({ source: retrySource || 'manual-retry' });
                },
                actionLabel: '重试',
            });
            triggerComposerLaunchFx('error');
        }

        function handleMobileVideoFileSelected(inputEl) {
            var selected = inputEl && inputEl.files && inputEl.files.length > 0
                ? inputEl.files[0]
                : null;
            if (!selected) {
                if (onFileSelected) {
                    try {
                        onFileSelected(null, inputEl);
                    } catch (_onFileClearError) {
                        // ignore callback errors
                    }
                }
                setTaskSubmitTip(uiCopy.composerIdleTip);
                return;
            }
            var decision = null;
            if (onFileSelected) {
                try {
                    decision = onFileSelected(selected, inputEl) || null;
                } catch (_callbackError) {
                    decision = null;
                }
            }
            var shouldAutoSubmit = !(decision && decision.autoSubmit === false);
            var tipMessage = (decision && typeof decision.tip === 'string' && decision.tip.trim())
                ? decision.tip.trim()
                : copyText('uploadingTip', '正在上传文件...');
            setTaskSubmitTip(tipMessage);
            if (!shouldAutoSubmit) {
                return;
            }
            triggerComposerAutoSubmit('file-selected').catch(function (error) {
                handleSubmitFailure(error, true, 'file-retry');
            });
        }

        function openMobileUploadPicker(event) {
            if (event) {
                if (typeof event.preventDefault === 'function') event.preventDefault();
                if (typeof event.stopPropagation === 'function') event.stopPropagation();
            }
            var fileInput = getFileInputNode();
            if (!fileInput || typeof fileInput.click !== 'function') {
                return false;
            }
            fileInput.click();
            return false;
        }

        function handleMobileSubmitClick(event) {
            if (event) {
                if (typeof event.preventDefault === 'function') event.preventDefault();
                if (typeof event.stopPropagation === 'function') event.stopPropagation();
            }
            submitTaskFromMobileForm().catch(function (error) {
                var fileInput = getFileInputNode();
                var hasFile = !!(fileInput && fileInput.files && fileInput.files.length > 0);
                handleSubmitFailure(error, hasFile, 'manual-retry');
            });
            return false;
        }

        // 做什么：把 Enter 提交绑定在一处。
        // 为什么：保持与 Android 一致的显式提交节奏，避免粘贴/失焦即触发。
        // 取舍：使用 data 标记防止重复绑定，简单可靠但不做复杂解绑。
        function bindInputAutoSubmit(inputNode) {
            if (!inputNode) return;
            if (inputNode.dataset && inputNode.dataset.composerAutoSubmitBound === '1') {
                return;
            }
            if (inputNode.dataset) {
                inputNode.dataset.composerAutoSubmitBound = '1';
            }
            inputNode.addEventListener('keydown', function (event) {
                if (event.key !== 'Enter') return;
                if (event.isComposing) return;
                if (event.shiftKey) return;
                handleMobileSubmitClick(event);
            });
        }

        return {
            applyComposerCopy: applyComposerCopy,
            setTaskSubmitTip: setTaskSubmitTip,
            triggerComposerSurfacePulse: triggerComposerSurfacePulse,
            triggerComposerLaunchFx: triggerComposerLaunchFx,
            triggerComposerAutoSubmit: triggerComposerAutoSubmit,
            handleMobileVideoFileSelected: handleMobileVideoFileSelected,
            openMobileUploadPicker: openMobileUploadPicker,
            handleMobileSubmitClick: handleMobileSubmitClick,
            bindInputAutoSubmit: bindInputAutoSubmit,
        };
    }

    global.createMobileComposerController = createMobileComposerController;
})(window);
