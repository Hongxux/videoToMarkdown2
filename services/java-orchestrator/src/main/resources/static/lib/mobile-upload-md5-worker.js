(function () {
    'use strict';

    function add32(a, b) {
        return (a + b) & 0xFFFFFFFF;
    }

    function cmn(q, a, b, x, s, t) {
        var n = add32(add32(a, q), add32(x, t));
        return add32((n << s) | (n >>> (32 - s)), b);
    }

    function ff(a, b, c, d, x, s, t) {
        return cmn((b & c) | ((~b) & d), a, b, x, s, t);
    }

    function gg(a, b, c, d, x, s, t) {
        return cmn((b & d) | (c & (~d)), a, b, x, s, t);
    }

    function hh(a, b, c, d, x, s, t) {
        return cmn(b ^ c ^ d, a, b, x, s, t);
    }

    function ii(a, b, c, d, x, s, t) {
        return cmn(c ^ (b | (~d)), a, b, x, s, t);
    }

    function md5cycle(state, block) {
        var a = state[0];
        var b = state[1];
        var c = state[2];
        var d = state[3];

        a = ff(a, b, c, d, block[0], 7, -680876936);
        d = ff(d, a, b, c, block[1], 12, -389564586);
        c = ff(c, d, a, b, block[2], 17, 606105819);
        b = ff(b, c, d, a, block[3], 22, -1044525330);
        a = ff(a, b, c, d, block[4], 7, -176418897);
        d = ff(d, a, b, c, block[5], 12, 1200080426);
        c = ff(c, d, a, b, block[6], 17, -1473231341);
        b = ff(b, c, d, a, block[7], 22, -45705983);
        a = ff(a, b, c, d, block[8], 7, 1770035416);
        d = ff(d, a, b, c, block[9], 12, -1958414417);
        c = ff(c, d, a, b, block[10], 17, -42063);
        b = ff(b, c, d, a, block[11], 22, -1990404162);
        a = ff(a, b, c, d, block[12], 7, 1804603682);
        d = ff(d, a, b, c, block[13], 12, -40341101);
        c = ff(c, d, a, b, block[14], 17, -1502002290);
        b = ff(b, c, d, a, block[15], 22, 1236535329);

        a = gg(a, b, c, d, block[1], 5, -165796510);
        d = gg(d, a, b, c, block[6], 9, -1069501632);
        c = gg(c, d, a, b, block[11], 14, 643717713);
        b = gg(b, c, d, a, block[0], 20, -373897302);
        a = gg(a, b, c, d, block[5], 5, -701558691);
        d = gg(d, a, b, c, block[10], 9, 38016083);
        c = gg(c, d, a, b, block[15], 14, -660478335);
        b = gg(b, c, d, a, block[4], 20, -405537848);
        a = gg(a, b, c, d, block[9], 5, 568446438);
        d = gg(d, a, b, c, block[14], 9, -1019803690);
        c = gg(c, d, a, b, block[3], 14, -187363961);
        b = gg(b, c, d, a, block[8], 20, 1163531501);
        a = gg(a, b, c, d, block[13], 5, -1444681467);
        d = gg(d, a, b, c, block[2], 9, -51403784);
        c = gg(c, d, a, b, block[7], 14, 1735328473);
        b = gg(b, c, d, a, block[12], 20, -1926607734);

        a = hh(a, b, c, d, block[5], 4, -378558);
        d = hh(d, a, b, c, block[8], 11, -2022574463);
        c = hh(c, d, a, b, block[11], 16, 1839030562);
        b = hh(b, c, d, a, block[14], 23, -35309556);
        a = hh(a, b, c, d, block[1], 4, -1530992060);
        d = hh(d, a, b, c, block[4], 11, 1272893353);
        c = hh(c, d, a, b, block[7], 16, -155497632);
        b = hh(b, c, d, a, block[10], 23, -1094730640);
        a = hh(a, b, c, d, block[13], 4, 681279174);
        d = hh(d, a, b, c, block[0], 11, -358537222);
        c = hh(c, d, a, b, block[3], 16, -722521979);
        b = hh(b, c, d, a, block[6], 23, 76029189);
        a = hh(a, b, c, d, block[9], 4, -640364487);
        d = hh(d, a, b, c, block[12], 11, -421815835);
        c = hh(c, d, a, b, block[15], 16, 530742520);
        b = hh(b, c, d, a, block[2], 23, -995338651);

        a = ii(a, b, c, d, block[0], 6, -198630844);
        d = ii(d, a, b, c, block[7], 10, 1126891415);
        c = ii(c, d, a, b, block[14], 15, -1416354905);
        b = ii(b, c, d, a, block[5], 21, -57434055);
        a = ii(a, b, c, d, block[12], 6, 1700485571);
        d = ii(d, a, b, c, block[3], 10, -1894986606);
        c = ii(c, d, a, b, block[10], 15, -1051523);
        b = ii(b, c, d, a, block[1], 21, -2054922799);
        a = ii(a, b, c, d, block[8], 6, 1873313359);
        d = ii(d, a, b, c, block[15], 10, -30611744);
        c = ii(c, d, a, b, block[6], 15, -1560198380);
        b = ii(b, c, d, a, block[13], 21, 1309151649);
        a = ii(a, b, c, d, block[4], 6, -145523070);
        d = ii(d, a, b, c, block[11], 10, -1120210379);
        c = ii(c, d, a, b, block[2], 15, 718787259);
        b = ii(b, c, d, a, block[9], 21, -343485551);

        state[0] = add32(a, state[0]);
        state[1] = add32(b, state[1]);
        state[2] = add32(c, state[2]);
        state[3] = add32(d, state[3]);
    }

    function bytesToWords(bytes, offset) {
        return [
            (bytes[offset] | (bytes[offset + 1] << 8) | (bytes[offset + 2] << 16) | (bytes[offset + 3] << 24)),
            (bytes[offset + 4] | (bytes[offset + 5] << 8) | (bytes[offset + 6] << 16) | (bytes[offset + 7] << 24)),
            (bytes[offset + 8] | (bytes[offset + 9] << 8) | (bytes[offset + 10] << 16) | (bytes[offset + 11] << 24)),
            (bytes[offset + 12] | (bytes[offset + 13] << 8) | (bytes[offset + 14] << 16) | (bytes[offset + 15] << 24)),
            (bytes[offset + 16] | (bytes[offset + 17] << 8) | (bytes[offset + 18] << 16) | (bytes[offset + 19] << 24)),
            (bytes[offset + 20] | (bytes[offset + 21] << 8) | (bytes[offset + 22] << 16) | (bytes[offset + 23] << 24)),
            (bytes[offset + 24] | (bytes[offset + 25] << 8) | (bytes[offset + 26] << 16) | (bytes[offset + 27] << 24)),
            (bytes[offset + 28] | (bytes[offset + 29] << 8) | (bytes[offset + 30] << 16) | (bytes[offset + 31] << 24)),
            (bytes[offset + 32] | (bytes[offset + 33] << 8) | (bytes[offset + 34] << 16) | (bytes[offset + 35] << 24)),
            (bytes[offset + 36] | (bytes[offset + 37] << 8) | (bytes[offset + 38] << 16) | (bytes[offset + 39] << 24)),
            (bytes[offset + 40] | (bytes[offset + 41] << 8) | (bytes[offset + 42] << 16) | (bytes[offset + 43] << 24)),
            (bytes[offset + 44] | (bytes[offset + 45] << 8) | (bytes[offset + 46] << 16) | (bytes[offset + 47] << 24)),
            (bytes[offset + 48] | (bytes[offset + 49] << 8) | (bytes[offset + 50] << 16) | (bytes[offset + 51] << 24)),
            (bytes[offset + 52] | (bytes[offset + 53] << 8) | (bytes[offset + 54] << 16) | (bytes[offset + 55] << 24)),
            (bytes[offset + 56] | (bytes[offset + 57] << 8) | (bytes[offset + 58] << 16) | (bytes[offset + 59] << 24)),
            (bytes[offset + 60] | (bytes[offset + 61] << 8) | (bytes[offset + 62] << 16) | (bytes[offset + 63] << 24))
        ];
    }

    function wordToHex(value) {
        var n = value >>> 0;
        var result = '';
        for (var i = 0; i < 4; i += 1) {
            var byte = (n >>> (i * 8)) & 0xFF;
            var hex = byte.toString(16);
            result += hex.length === 1 ? '0' + hex : hex;
        }
        return result;
    }

    var state = null;
    var tail = null;
    var tailLength = 0;
    var totalBytes = 0;

    function resetState() {
        state = [1732584193, -271733879, -1732584194, 271733878];
        tail = new Uint8Array(64);
        tailLength = 0;
        totalBytes = 0;
    }

    function processBlock(blockBytes, offset) {
        var words = bytesToWords(blockBytes, offset);
        md5cycle(state, words);
    }

    function appendChunk(chunkBytes) {
        if (!(chunkBytes instanceof Uint8Array) || chunkBytes.length <= 0) {
            return;
        }
        totalBytes += chunkBytes.length;
        var index = 0;

        if (tailLength > 0) {
            var needed = 64 - tailLength;
            var toCopy = Math.min(needed, chunkBytes.length);
            tail.set(chunkBytes.subarray(0, toCopy), tailLength);
            tailLength += toCopy;
            index += toCopy;
            if (tailLength === 64) {
                processBlock(tail, 0);
                tailLength = 0;
            }
        }

        while (index + 64 <= chunkBytes.length) {
            processBlock(chunkBytes, index);
            index += 64;
        }

        if (index < chunkBytes.length) {
            tail.set(chunkBytes.subarray(index), 0);
            tailLength = chunkBytes.length - index;
        }
    }

    function finishDigest() {
        var finalBuffer = new Uint8Array(128);
        finalBuffer.set(tail.subarray(0, tailLength), 0);
        var index = tailLength;
        finalBuffer[index++] = 0x80;

        var mod = index % 64;
        var padZeroCount = mod <= 56 ? (56 - mod) : (56 + (64 - mod));
        index += padZeroCount;

        var low = (totalBytes << 3) >>> 0;
        var high = Math.floor(totalBytes / 0x20000000) >>> 0;

        finalBuffer[index++] = low & 0xFF;
        finalBuffer[index++] = (low >>> 8) & 0xFF;
        finalBuffer[index++] = (low >>> 16) & 0xFF;
        finalBuffer[index++] = (low >>> 24) & 0xFF;
        finalBuffer[index++] = high & 0xFF;
        finalBuffer[index++] = (high >>> 8) & 0xFF;
        finalBuffer[index++] = (high >>> 16) & 0xFF;
        finalBuffer[index++] = (high >>> 24) & 0xFF;

        for (var offset = 0; offset < index; offset += 64) {
            processBlock(finalBuffer, offset);
        }

        return wordToHex(state[0]) + wordToHex(state[1]) + wordToHex(state[2]) + wordToHex(state[3]);
    }

    resetState();

    var scope = typeof self !== 'undefined' ? self : globalThis;

    scope.onmessage = function (event) {
        var payload = event && event.data ? event.data : {};
        if (payload.type === 'reset') {
            resetState();
            scope.postMessage({ type: 'ready' });
            return;
        }
        if (payload.type !== 'chunk') {
            scope.postMessage({ type: 'error', message: 'unsupported message type' });
            return;
        }
        try {
            var chunk = payload.chunk instanceof ArrayBuffer
                ? new Uint8Array(payload.chunk)
                : new Uint8Array(0);
            appendChunk(chunk);
            if (payload.last) {
                var md5 = finishDigest();
                scope.postMessage({ type: 'done', md5: md5 });
                resetState();
                return;
            }
            scope.postMessage({ type: 'progress', processedBytes: totalBytes });
        } catch (error) {
            scope.postMessage({
                type: 'error',
                message: error && error.message ? String(error.message) : 'md5 worker failed'
            });
            resetState();
        }
    };
})();
