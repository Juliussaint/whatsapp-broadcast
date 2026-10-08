// =============================================================================
// server.js — WhatsApp Broadcaster (Optimized)
// =============================================================================
const util = require('util');

'use strict';

require('dotenv').config();

// --- REQUIRED NODE.JS MODULES ---
const express      = require('express');
const bodyParser   = require('body-parser');
const fs           = require('fs');
const path         = require('path');
const multer       = require('multer');
const xlsx         = require('xlsx');
const crypto       = require('crypto');
const {
    default: makeWASocket,
    useMultiFileAuthState,
    DisconnectReason,
    fetchLatestBaileysVersion
} = require('@whiskeysockets/baileys');
const qrcodeTerminal = require('qrcode-terminal');
const qrcode         = require('qrcode');
const { Boom }       = require('@hapi/boom');

// =============================================================================
// CONFIGURATION  (override any of these via .env)
// =============================================================================
const CONFIG = {
    PORT             : parseInt(process.env.PORT)              || 3000,
    HOST             : process.env.HOST                        || '0.0.0.0',
    MIN_DELAY_MS     : parseInt(process.env.MIN_DELAY_MS)      || 20_000,
    MAX_DELAY_MS     : parseInt(process.env.MAX_DELAY_MS)      || 60_000,
    SESSION_FOLDER   : process.env.SESSION_FOLDER              || 'auth_info_baileys',
    API_KEY          : process.env.API_KEY                     || null,   // set in .env to enable auth
    MAX_FILE_SIZE_MB : parseInt(process.env.MAX_FILE_SIZE_MB)  || 50,
    MAX_SSE_CLIENTS  : parseInt(process.env.MAX_SSE_CLIENTS)   || 50,
    REPORT_TTL_MS    : parseInt(process.env.REPORT_TTL_MS)     || 86_400_000, // 24 h
    BROADCAST_TIMEOUT_MS: parseInt(process.env.BROADCAST_TIMEOUT_MS) || 14_400_000, // 4 h
    RECONNECT_DELAY_MS  : parseInt(process.env.RECONNECT_DELAY_MS)   || 5_000,
    SEND_RETRIES     : parseInt(process.env.SEND_RETRIES)      || 2,
    VALID_COUNTRY_CODES: (process.env.VALID_COUNTRY_CODES || '1,44,49,33,55,60,61,62,65,81,82,91').split(','),
};

// =============================================================================
// SERVER SETUP
// =============================================================================
const app = express();

app.use(express.static('public'));
app.use(bodyParser.json());

// =============================================================================
// MULTER  (memory storage, size + file count limits)
// =============================================================================
const upload = multer({
    storage: multer.memoryStorage(),
    limits : {
        fileSize: CONFIG.MAX_FILE_SIZE_MB * 1024 * 1024,
        files   : 1,
    },
});

// =============================================================================
// GLOBAL STATE
// =============================================================================
let sock             = null;
let connectionStatus = 'DISCONNECTED';
let qrCode           = null;          // Base64 PNG
let sseClients       = [];

// Broadcast concurrency
let isBroadcasting      = false;
let broadcastStartedAt  = null;

// In-memory report store  { broadcastID -> reportObject }
const broadcastReports = new Map();

// =============================================================================
// LOGGING  (SSE-enabled, levelled)
// =============================================================================

/**
 * Levels that are forwarded to connected browser clients.
 * 'debug' and 'trace' are suppressed from the SSE stream to avoid flooding.
 */
const SSE_LEVELS = new Set(['info', 'warn', 'error', 'success', 'baileys', 'queue']);

function log(level = 'info', message, ...args) {
    const timestamp = new Date().toLocaleTimeString();
    let logMessage  = `[${timestamp}] [${level.toUpperCase()}] ${message}`;

    if (args.length > 0) {
        logMessage += ' ' + args
            .map(a => (typeof a === 'object' && a !== null) ? (a.message || JSON.stringify(a)) : String(a))
            .join(' ');
    }

    // Console output for all levels
    const consoleFn = console[level] || console.log;
    consoleFn(logMessage);

    // SSE — only forward non-noisy levels
    if (SSE_LEVELS.has(level)) {
        const payload = JSON.stringify({ level, message: logMessage });
        sseClients.forEach(client => client.write(`data: ${payload}\n\n`));
    }
}

function formatError(err) {
    if (!err) return 'Unknown error';

    if (err instanceof Error) {
        return `${err.name}: ${err.message}\n${err.stack}`;
    }

    return util.inspect(err, {
        depth: null,
        colors: false,
        compact: false
    });
}

// Minimal Baileys-compatible logger (suppresses debug/trace)
const baileysLogger = {
    info : (msg, ...a) => log('baileys', msg, ...a),
    warn : (msg, ...a) => log('warn',    msg, ...a),
    error: (msg, ...a) => log('error',   msg, ...a),
    debug: ()          => {},   // suppressed
    trace: ()          => {},   // suppressed
    child: ()          => baileysLogger,
};

// =============================================================================
// HELPERS
// =============================================================================

const sleep    = ms  => new Promise(r => setTimeout(r, ms));
const sleepRandom = async () => {
    const ms = Math.floor(Math.random() * (CONFIG.MAX_DELAY_MS - CONFIG.MIN_DELAY_MS + 1)) + CONFIG.MIN_DELAY_MS;
    log('queue', `Waiting ${(ms / 1000).toFixed(1)}s before next send...`);
    await sleep(ms);
};

/** Remove the Baileys session folder */
function cleanSession() {
    if (fs.existsSync(CONFIG.SESSION_FOLDER)) {
        fs.rmSync(CONFIG.SESSION_FOLDER, { recursive: true, force: true });
        log('baileys', `Session folder '${CONFIG.SESSION_FOLDER}' cleared.`);
    }
}

/**
 * Normalise a raw phone string to a JID (`<digits>@s.whatsapp.net`).
 * Returns null if the number is invalid.
 */
function formatNumberToJID(number, countryCode) {
    if (!number || !countryCode) return null;

    let num = String(number).trim();

    // Strip everything except digits (keep a leading + for now)
    if (num.startsWith('+')) {
        num = num.replace(/[^0-9]/g, '');
    } else {
        num = num.replace(/[^0-9]/g, '');
        if (num.startsWith('0')) {
            num = countryCode + num.substring(1);
        } else if (!num.startsWith(countryCode)) {
            num = countryCode + num;
        }
    }

    // Guard: prevent double country-code prepend
    if (num.startsWith(countryCode + countryCode)) {
        num = num.substring(countryCode.length);
    }

    if (num.length < 8 || num.length > 15) {
        log('warn', `[JID] Skipping number (bad length): ${number} → ${num}`);
        return null;
    }
    if (!num.startsWith(countryCode)) {
        log('warn', `[JID] Sanitisation failed for ${number} — doesn't start with ${countryCode}`);
        return null;
    }

    return `${num}@s.whatsapp.net`;
}

/**
 * Returns true when the broadcast lock has been held for longer than
 * BROADCAST_TIMEOUT_MS — safety valve for crashes / hung loops.
 */
function isBroadcastStuck() {
    return isBroadcasting
        && broadcastStartedAt !== null
        && (Date.now() - broadcastStartedAt > CONFIG.BROADCAST_TIMEOUT_MS);
}

/** Acquire the broadcast lock */
function lockBroadcast() {
    isBroadcasting     = true;
    broadcastStartedAt = Date.now();
}

/** Release the broadcast lock */
function unlockBroadcast() {
    isBroadcasting     = false;
    broadcastStartedAt = null;
}

/** Schedule automatic deletion of a report after TTL */
function scheduleReportCleanup(broadcastID) {
    setTimeout(() => {
        broadcastReports.delete(broadcastID);
        log('info', `Report ${broadcastID} expired and removed from memory.`);
    }, CONFIG.REPORT_TTL_MS);
}

// =============================================================================
// MESSAGE SENDING
// =============================================================================

/**
 * Build and send a single message payload (text / image / video).
 */
async function sendMessagePayload(jid, message, mediaBuffer, mimeType) {
    let payload;
    const lowerMime = mimeType ? mimeType.toLowerCase() : '';

    if (mediaBuffer) {
        if (lowerMime.startsWith('image/')) {
            payload = { image: mediaBuffer, caption: message };
        } else if (lowerMime.startsWith('video/')) {
            payload = { video: mediaBuffer, caption: message };
        } else {
            log('warn', `Unsupported media type '${mimeType}'. Sending text-only.`);
            payload = { text: `[Unsupported Media] ${message}` };
        }
    } else {
        payload = { text: message };
    }

    const result = await sock.sendMessage(jid, payload);

    console.dir(result, { depth: null });

    return result;
}

/**
 * Wrapper around sendMessagePayload with exponential-backoff retry.
 */
async function sendWithRetry(jid, message, mediaBuffer, mimeType) {
    let lastError;
    for (let attempt = 0; attempt <= CONFIG.SEND_RETRIES; attempt++) {
        try {
            await sendMessagePayload(jid, message, mediaBuffer, mimeType);
            return; // success
        } catch (err) {
            lastError = err;
            if (attempt < CONFIG.SEND_RETRIES) {
                const backoff = 3000 * (attempt + 1);
                log('warn', `Send attempt ${attempt + 1} failed for ${jid}. Retrying in ${backoff / 1000}s…`);
                await sleep(backoff);
            }
        }
    }
    throw lastError;
}

// =============================================================================
// BROADCAST RUNNERS
// =============================================================================

/**
 * Shared pre-send WhatsApp number validation.
 * Returns true if the JID is a registered WA account, false otherwise.
 */
async function verifyJID(jid) {
    try {
        const [result] = await sock.onWhatsApp(jid);
        return result && result.exists;
    } catch (err) {
        log('error', `Failed to verify ${jid}: ${err.message}`);
        return false; // treat as unverifiable → skip
    }
}

/**
 * Update the in-progress report counter so the frontend polling endpoint
 * can show live progress.
 */
function pushResult(reportData, entry) {
    reportData.results.push(entry);
    // Keep processed count in sync for live polling
    reportData.processed = reportData.results.length;
}

async function runPersonalizedBroadcast(recipients, message, mediaBuffer, mimeType, broadcastID) {
    const reportData = broadcastReports.get(broadcastID);
    if (!reportData) {
        log('error', `Report ${broadcastID} not found. Aborting.`);
        unlockBroadcast();
        return;
    }

    reportData.status = 'RUNNING';
    log('info', `=== PERSONALIZED BROADCAST [${broadcastID}] → ${recipients.length} recipients ===`);

    for (let i = 0; i < recipients.length; i++) {
        const { name, jid } = recipients[i];

        if (i > 0) await sleepRandom();

        const logPrefix = `[${i + 1}/${recipients.length}] ${jid} (${name})`;

        const exists = await verifyJID(jid);
        if (!exists) {
            log('warn', `⚠️  Skipping ${logPrefix}: not a registered WhatsApp number.`);
            pushResult(reportData, { name, jid, status: 'SKIPPED', reason: 'Not a registered WhatsApp number.' });
            continue;
        }

        const personalizedMsg = message.replace(/{name}/gi, name);

        try {
            await sendWithRetry(jid, personalizedMsg, mediaBuffer, mimeType);
            log('success', `✅ Sent to ${logPrefix}`);
            pushResult(reportData, { name, jid, status: 'SENT', reason: '' });
        } catch (err) {
            log('error', `🔴 Failed to send to ${logPrefix}: ${err.message}`);
            pushResult(reportData, { name, jid, status: 'FAILED', reason: err.message.substring(0, 100) });
        }
    }

    finalizeBroadcast(reportData, broadcastID);
}

async function runManualBroadcast(jids, message, mediaBuffer, mimeType, broadcastID) {
    const reportData = broadcastReports.get(broadcastID);
    if (!reportData) {
        log('error', `Report ${broadcastID} not found. Aborting.`);
        unlockBroadcast();
        return;
    }

    reportData.status = 'RUNNING';
    log('info', `=== MANUAL BROADCAST [${broadcastID}] → ${jids.length} contacts ===`);

    for (let i = 0; i < jids.length; i++) {
        const jid = jids[i];

        if (i > 0) await sleepRandom();

        const logPrefix = `[${i + 1}/${jids.length}] ${jid}`;

        const exists = await verifyJID(jid);
        if (!exists) {
            log('warn', `⚠️  Skipping ${logPrefix}: not a registered WhatsApp number.`);
            pushResult(reportData, { name: 'N/A', jid, status: 'SKIPPED', reason: 'Not a registered WhatsApp number.' });
            continue;
        }

        try {
            await sendWithRetry(jid, message, mediaBuffer, mimeType);
            log('success', `✅ Sent to ${logPrefix}`);
            pushResult(reportData, { name: 'N/A', jid, status: 'SENT', reason: '' });
        } catch (err) {
            log('error', `🔴 Failed to send to ${logPrefix}: ${err.message}`);
            pushResult(reportData, { name: 'N/A', jid, status: 'FAILED', reason: err.message.substring(0, 100) });
        }
    }

    finalizeBroadcast(reportData, broadcastID);
}

function finalizeBroadcast(reportData, broadcastID) {
    reportData.status  = 'COMPLETE';
    reportData.endTime = new Date();
    const elapsed      = ((reportData.endTime - reportData.startTime) / 1000).toFixed(1);

    unlockBroadcast();
    scheduleReportCleanup(broadcastID);

    const sent    = reportData.results.filter(r => r.status === 'SENT').length;
    const failed  = reportData.results.filter(r => r.status === 'FAILED').length;
    const skipped = reportData.results.filter(r => r.status === 'SKIPPED').length;

    log('info', `=== BROADCAST [${broadcastID}] COMPLETE ===`);
    log('info', `Total: ${reportData.totalContacts} | Sent: ${sent} | Failed: ${failed} | Skipped: ${skipped} | Time: ${elapsed}s`);
}

// =============================================================================
// WHATSAPP CONNECTION
// =============================================================================

async function connectToWhatsApp() {
    const { state, saveCreds } = await useMultiFileAuthState(CONFIG.SESSION_FOLDER);

    let version;
    try {
        const fetched = await fetchLatestBaileysVersion();
        version       = fetched.version;
        log('baileys', `Using WhatsApp Web version: ${version.join('.')}`);
    } catch (e) {
        version = [2, 2413, 51];
        log('warn', `Could not fetch latest WA version. Falling back to ${version.join('.')}`);
    }

    sock = makeWASocket({
        auth              : state,
        printQRInTerminal : false,
        logger            : baileysLogger,
        browser           : ['Linux', 'Chrome', '124.0.0'],
        version,
        getMessage        : async () => ({}),
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            try {
                const dataUrl = await qrcode.toDataURL(qr, { type: 'image/png' });
                qrCode           = dataUrl.split(',')[1];
                connectionStatus = 'SCANNING_QR';
                log('baileys', 'New QR code generated. Status: SCANNING_QR.');
                qrcodeTerminal.generate(qr, { small: true });
            } catch (e) {
                log('error', 'Failed to generate QR image:', e.message);
                qrCode           = null;
                connectionStatus = 'DISCONNECTED';
            }
        }

        if (connection === 'close') {
            const statusCode = (new Boom(lastDisconnect?.error)).output?.statusCode;

            if (statusCode === DisconnectReason.loggedOut) {
                connectionStatus = 'LOGGED_OUT';
                qrCode           = null;
                log('baileys', 'Logged out. Clearing session and reconnecting…');
                cleanSession();
                connectToWhatsApp();
            } else {
                connectionStatus = 'RECONNECTING';
                log('baileys', `Connection closed (code: ${statusCode ?? 'unknown'}). Reconnecting in ${CONFIG.RECONNECT_DELAY_MS / 1000}s…`);
                setTimeout(connectToWhatsApp, CONFIG.RECONNECT_DELAY_MS);
            }
        } else if (connection === 'open') {
            connectionStatus = 'CONNECTED';
            qrCode           = null;
            log('success', '✅ WhatsApp connected. Ready to broadcast.');
        }
    });

    return sock;
}

// =============================================================================
// MIDDLEWARE
// =============================================================================

/** Optional API key guard — only active when CONFIG.API_KEY is set */
function apiKeyMiddleware(req, res, next) {
    if (!CONFIG.API_KEY) return next(); // auth disabled
    const provided = req.headers['x-api-key'];
    if (provided !== CONFIG.API_KEY) {
        return res.status(401).json({ success: false, error: 'Unauthorized.' });
    }
    next();
}

/** Validate that the submitted countryCode is in our whitelist */
function validateCountryCode(countryCode) {
    return CONFIG.VALID_COUNTRY_CODES.includes(String(countryCode));
}

app.use('/api', apiKeyMiddleware);

// =============================================================================
// REST API ENDPOINTS
// =============================================================================

// ---------------------------------------------------------------------------
// 0. SSE Live Log Stream
// ---------------------------------------------------------------------------
app.get('/api/logs', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection',    'keep-alive');
    res.flushHeaders();

    // Cap SSE connections — drop oldest when limit is hit
    if (sseClients.length >= CONFIG.MAX_SSE_CLIENTS) {
        const oldest = sseClients.shift();
        try { oldest.end(); } catch (_) {}
        log('warn', `SSE client cap (${CONFIG.MAX_SSE_CLIENTS}) reached. Dropped oldest connection.`);
    }

    sseClients.push(res);
    const welcome = JSON.stringify({ level: 'info', message: `[${new Date().toLocaleTimeString()}] [INFO] Live log stream connected.` });
    res.write(`data: ${welcome}\n\n`);

    req.on('close', () => {
        sseClients = sseClients.filter(c => c !== res);
    });
});

// ---------------------------------------------------------------------------
// 1. Status
// ---------------------------------------------------------------------------
app.get('/api/status', (req, res) => {
    // Auto-release stuck broadcast lock
    if (isBroadcastStuck()) {
        log('warn', 'Broadcast lock was stuck. Auto-releasing.');
        unlockBroadcast();
    }

    res.json({ status: connectionStatus, qrCode });
});

// ---------------------------------------------------------------------------
// 2. Start Broadcast
// ---------------------------------------------------------------------------
app.post('/api/broadcast', upload.single('media-file'), async (req, res) => {
    // Concurrency check (with stuck-lock safety)
    if (isBroadcasting && !isBroadcastStuck()) {
        return res.status(429).json({
            success: false,
            error  : 'A broadcast is already running. Please wait for it to finish.',
        });
    }
    if (isBroadcastStuck()) {
        log('warn', 'Auto-releasing stuck broadcast lock before new broadcast.');
        unlockBroadcast();
    }

    if (connectionStatus !== 'CONNECTED' || !sock) {
        return res.status(400).json({
            success: false,
            error  : `WhatsApp is not connected. Current status: ${connectionStatus}`,
        });
    }

    // --- Parse body ---
    let recipientsData, rawManualContacts, countryCode, message;
    try {
        message      = req.body.message || '';
        countryCode  = req.body.countryCode;

        if (req.body.recipients) {
            recipientsData    = JSON.parse(req.body.recipients);
        } else if (req.body.contacts) {
            rawManualContacts = req.body.contacts;
        }
    } catch {
        return res.status(400).json({ success: false, error: 'Invalid form data.' });
    }

    // --- Validation ---
    if (!validateCountryCode(countryCode)) {
        return res.status(400).json({ success: false, error: `Invalid country code: ${countryCode}` });
    }

    const mediaBuffer = req.file ? req.file.buffer  : null;
    const mimeType    = req.file ? req.file.mimetype : null;

    if (!message && !mediaBuffer) {
        return res.status(400).json({ success: false, error: 'A message or media file must be provided.' });
    }

    // --- Build recipient list ---
    const broadcastID    = crypto.randomUUID().substring(0, 8);
    let   totalContacts  = 0;
    let   isPersonalized = false;
    let   jids           = [];

    if (Array.isArray(recipientsData) && recipientsData.length > 0) {
        totalContacts  = recipientsData.length;
        isPersonalized = true;
    } else if (rawManualContacts) {
        jids = rawManualContacts
            .split('\n')
            .map(n => formatNumberToJID(n.trim(), countryCode))
            .filter(Boolean);
        totalContacts = jids.length;
    }

    if (totalContacts === 0) {
        return res.status(400).json({ success: false, error: 'No valid contacts provided.' });
    }

    // --- Create report entry ---
    broadcastReports.set(broadcastID, {
        status       : 'PENDING',
        results      : [],
        processed    : 0,
        startTime    : new Date(),
        endTime      : null,
        totalContacts,
        timestamp    : new Date(),
    });

    // --- Lock & respond immediately ---
    lockBroadcast();

    const modeLabel = isPersonalized ? 'personalized' : 'manual';
    log('info', `Broadcast [${broadcastID}] queued — mode: ${modeLabel}, contacts: ${totalContacts}`);

    res.json({
        success    : true,
        message    : `${modeLabel.charAt(0).toUpperCase() + modeLabel.slice(1)} broadcast started for ${totalContacts} contact(s).`,
        broadcastID,
    });

    // --- Run asynchronously (do not await) ---
    if (isPersonalized) {
        runPersonalizedBroadcast(recipientsData, message, mediaBuffer, mimeType, broadcastID);
    } else {
        runManualBroadcast(jids, message, mediaBuffer, mimeType, broadcastID);
    }
});

// ---------------------------------------------------------------------------
// 3. Upload & Parse Contacts Excel
// ---------------------------------------------------------------------------
app.post('/api/upload-contacts', upload.single('contacts-file'), async (req, res) => {
    const countryCode = req.body.countryCode;

    if (!req.file) {
        return res.status(400).json({ success: false, error: 'No file uploaded.' });
    }
    if (!validateCountryCode(countryCode)) {
        return res.status(400).json({ success: false, error: `Invalid country code: ${countryCode}` });
    }

    try {
        const workbook  = xlsx.read(req.file.buffer, { type: 'buffer' });
        const sheet     = workbook.Sheets[workbook.SheetNames[0]];
        const data      = xlsx.utils.sheet_to_json(sheet, { header: 1 });

        if (data.length <= 1) {
            return res.status(400).json({ success: false, error: 'Excel file is empty or only contains a header row.' });
        }

        const header         = data[0];
        const nameColIndex   = header.findIndex(h => /name/i.test(h));
        const numberColIndex = header.findIndex(h => /number|phone|hp/i.test(h));

        if (nameColIndex === -1 || numberColIndex === -1) {
            return res.status(400).json({
                success: false,
                error  : 'Required columns not found. Excel must have a "Name" column and a "Number", "Phone", or "HP" column.',
            });
        }

        const recipients = [];
        for (let i = 1; i < data.length; i++) {
            const row    = data[i];
            const name   = String(row[nameColIndex] || 'Unknown').trim();
            const jid    = formatNumberToJID(row[numberColIndex], countryCode);
            if (jid) recipients.push({ name, jid });
        }

        log('info', `Contacts upload: ${recipients.length} valid contacts parsed from ${req.file.originalname}.`);
        res.json({ success: true, message: `Processed ${recipients.length} valid contact(s).`, recipients });

    } catch (err) {
        log('error', 'Error processing Excel file:', err.message);
        res.status(500).json({ success: false, error: 'Failed to process the Excel file.' });
    }
});

// ---------------------------------------------------------------------------
// 4. Logout
// ---------------------------------------------------------------------------
app.post('/api/logout', async (req, res) => {
    if (!sock) {
        return res.status(400).json({ success: false, error: 'Not connected.' });
    }

    try {
        await sock.logout();
    } catch (err) {
        log('warn', 'sock.logout() threw (non-fatal):', err.message);
    }

    sock             = null;
    connectionStatus = 'LOGGED_OUT';
    qrCode           = null;
    unlockBroadcast(); // release any lock on logout

    cleanSession();
    connectToWhatsApp();

    log('info', 'User logged out successfully.');
    res.json({ success: true, message: 'Logged out successfully.' });
});

// ---------------------------------------------------------------------------
// 5. Download Broadcast Report (CSV)
// ---------------------------------------------------------------------------
app.get('/api/report/download', (req, res) => {
    const { id } = req.query;
    if (!id) return res.status(400).send('Missing report ID.');

    const report = broadcastReports.get(id);
    if (!report)                        return res.status(404).send('Report not found.');
    if (report.status !== 'COMPLETE')   return res.status(202).send('Report is still being generated.');

    const rows = ['"Name","JID","Status","Reason"'];
    report.results.forEach(({ name, jid, status, reason }) => {
        const safe = s => `"${String(s).replace(/"/g, '""')}"`;
        rows.push([safe(name), safe(jid), safe(status), safe(reason)].join(','));
    });

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="broadcast_report_${id}.csv"`);
    res.status(200).send(rows.join('\n'));
});

// ---------------------------------------------------------------------------
// 6. Get Report Status (JSON — used for frontend polling)
// ---------------------------------------------------------------------------
app.get('/api/report/:id', (req, res) => {
    const report = broadcastReports.get(req.params.id);
    if (!report) {
        return res.status(404).json({ success: false, error: 'Report not found.' });
    }

    const sent    = report.results.filter(r => r.status === 'SENT').length;
    const failed  = report.results.filter(r => r.status === 'FAILED').length;
    const skipped = report.results.filter(r => r.status === 'SKIPPED').length;
    const elapsed = report.endTime
        ? ((report.endTime - report.startTime) / 1000).toFixed(1)
        : ((Date.now()     - report.startTime) / 1000).toFixed(1); // live elapsed

    res.json({
        success: true,
        status : report.status,
        summary: {
            total    : report.totalContacts,
            processed: report.processed || report.results.length,
            success  : sent,
            failed,
            skipped,
            time     : report.status === 'COMPLETE' ? `${elapsed}s` : 'Running…',
        },
    });
});

// =============================================================================
// HTML ENTRY POINT
// =============================================================================
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'app_dashboard.html'));
});

// =============================================================================
// GRACEFUL SHUTDOWN
// =============================================================================
async function shutdown(signal) {
    log('info', `Received ${signal}. Shutting down gracefully…`);

    // Mark any running broadcast as interrupted
    broadcastReports.forEach((report, id) => {
        if (report.status === 'RUNNING' || report.status === 'PENDING') {
            report.status  = 'INTERRUPTED';
            report.endTime = new Date();
            log('warn', `Broadcast [${id}] marked as INTERRUPTED due to shutdown.`);
        }
    });

    // Disconnect WhatsApp socket cleanly
    if (sock) {
        try { sock.end(undefined); } catch (_) {}
    }

    // Close SSE connections
    sseClients.forEach(c => { try { c.end(); } catch (_) {} });

    server.close(() => {
        log('info', 'HTTP server closed. Exiting.');
        process.exit(0);
    });

    // Force-exit after 10 s if something hangs
    setTimeout(() => {
        log('error', 'Forced exit after shutdown timeout.');
        process.exit(1);
    }, 10_000);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));

// =============================================================================
// STARTUP
// =============================================================================
const server = app.listen(CONFIG.PORT, CONFIG.HOST, () => {
    log('info', `Server running at http://${CONFIG.HOST}:${CONFIG.PORT}`);
    if (!CONFIG.API_KEY) {
        log('warn', 'API_KEY is not set. All /api routes are publicly accessible.');
    }
    connectToWhatsApp();
});