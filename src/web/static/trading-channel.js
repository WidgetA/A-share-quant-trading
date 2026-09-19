// The browser selects a channel; credentials and protocol translation stay on the server.
window.tradingChannel = {backend: 'miniqmt', channel: null};

async function channelFetch(url, options = {}) {
    if (typeof tradingFetch === 'function') return tradingFetch(url, options);
    let key = '';
    try { key = localStorage.getItem('tradingApiKey') || ''; } catch (_) {}
    return fetch(url, {...options, headers: {...options.headers, ...(key ? {'X-API-Key': key} : {})}});
}

window.channelLoaded = (async () => {
    try {
        const resp = await channelFetch('/api/settings/trading-channel');
        if (!resp.ok) throw new Error('无法读取交易通道');
        const data = await resp.json();
        window.tradingChannel = data;
        const select = document.getElementById('tradingChannelSelect');
        if (select) select.value = data.backend;
        const label = document.getElementById('tradingChannelCurrent');
        if (label) label.textContent = '当前使用：' + (data.backend === 'qmt' ? 'QMT' : 'miniQMT');
        const qmt = data.profiles.qmt;
        for (const field of ['url', 'instance_id', 'key_id', 'ca_file']) {
            const input = document.getElementById('qmt_' + field);
            if (input) input.value = qmt[field] || '';
        }
        const secret = document.getElementById('qmt_secret');
        if (secret && qmt.configured) secret.placeholder = '已保存；留空保留原密钥';
        const hint = document.getElementById('channelBatchHint');
        if (hint) hint.textContent = data.backend === 'qmt'
            ? 'QMT：按金额和参考价换算股数，每只提交一次市价委托，未成交余量撤销。'
            : 'miniQMT：市价五档转撤，按现有规则重试，余量转参考价限价。';
        return data;
    } catch (error) {
        const label = document.getElementById('tradingChannelCurrent');
        if (label) label.textContent = error.message;
        return null;
    }
})();

async function switchTradingChannel() {
    const select = document.getElementById('tradingChannelSelect');
    const message = document.getElementById('tradingChannelCurrent');
    const button = document.getElementById('tradingChannelApply');
    button.disabled = true;
    try {
        const resp = await channelFetch('/api/settings/trading-channel', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({backend: select.value})
        });
        const data = await resp.json();
        if (!resp.ok) throw new Error(data.detail || '切换失败');
        // Reload account, holdings, orders, and all action contexts together.
        location.reload();
    } catch (error) {
        message.textContent = error.message;
        select.value = window.tradingChannel.backend;
    } finally { button.disabled = false; }
}

async function saveQmtChannel(testOnly = false) {
    const body = {};
    for (const field of ['url', 'instance_id', 'key_id', 'secret', 'ca_file']) {
        body[field] = document.getElementById('qmt_' + field).value.trim();
    }
    const message = document.getElementById('qmtConfigMessage');
    message.textContent = testOnly ? '正在测试连接…' : '正在保存…';
    try {
        const resp = await channelFetch('/api/settings/trading-channel/qmt' + (testOnly ? '/test' : ''), {
            method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)
        });
        const data = await resp.json();
        message.textContent = data.message || data.detail || '请求失败';
        if (resp.ok && data.success && !testOnly) {
            document.getElementById('qmt_secret').value = '';
            document.getElementById('qmt_secret').placeholder = '已保存；留空保留原密钥';
        }
    } catch (_) { message.textContent = '请求未完成，请检查连接'; }
}

async function channelOrderPayload(payload, action) {
    if (!await window.channelLoaded || !window.tradingChannel.channel) {
        throw new Error('交易通道不可用，请先检查设置');
    }
    const channel = window.tradingChannel.channel;
    const signature = JSON.stringify({channel, action, payload});
    const key = 'brokerRequest:' + action;
    let saved;
    try { saved = JSON.parse(sessionStorage.getItem(key)); } catch (_) {}
    if (!saved || saved.signature !== signature) {
        const bytes = crypto.getRandomValues(new Uint8Array(16));
        saved = {signature, id: Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('')};
        sessionStorage.setItem(key, JSON.stringify(saved));
    }
    return {...payload, request_id: saved.id, channel};
}

function completeChannelRequest(action) {
    sessionStorage.removeItem('brokerRequest:' + action);
}

function tradeStateLabel(state) {
    const labels = {RECEIVED: '代理已接收', SUBMITTING: '正在提交', AWAITING_BROKER: '等待券商回报',
        UNKNOWN: '结果未确认', UNCONFIRMED: '结果未确认', PENDING: '待处理', NEW: '待处理',
        PENDING_NEW: '等待券商回报', ACCEPTED: '券商已受理', FILLED: '全部成交',
        PARTIALLY_FILLED: '部分成交', PARTIALLY_CANCELLED: '部分成交、余量已撤',
        CANCELLED: '已撤单', REJECTED: '已拒单', EXPIRED: '未发送、已失效', NOT_SENT: '未发送',
        PENDING_CANCEL: '撤单处理中', CANCEL_CONFIRMED: '已确认撤单', CANCEL_OBSOLETE: '撤单已不适用'};
    return labels[state] || state || '结果未确认';
}
