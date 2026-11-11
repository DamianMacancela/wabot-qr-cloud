/**
 * WaBot.AI 2079 EDGE — wa-qr-server/server.js (SOLUCIÓN DEFINITIVA AL BLOQUEO "CONECTANDO..." DEL QR)
 *
 * ¿POR QUÉ EL CELULAR SE QUEDABA EN "CONECTANDO..." Y DABA ERROR AL ESCANEAR EL QR?
 * 1. En el protocolo de WhatsApp (Baileys), apenas escaneas el QR con tu celular, WhatsApp guarda
 *    la llave en `creds.json` y cierra el socket con el código `515 (DisconnectReason.restartRequired)`
 *    exigiendo que el servidor reconecte en < 500ms usando ese `creds.json` local recién creado.
 * 2. El error ocurría por dos causas en ese instante crítico:
 *    a) `restoreAuthFromSupabase()` se ejecutaba otra vez en la reconexión del código `515` y
 *       SOBRESCRIBÍA el `creds.json` recién vinculado con los datos viejos de Supabase.
 *    b) `backupAuthToSupabase()` se ejecutaba de forma síncrona dentro de `creds.update` (que se
 *       dispara ~10 veces en 1 segundo durante el escaneo), bloqueando el hilo de Node.js en Render
 *       y haciendo que el celular expirara por timeout en "Conectando...".
 *
 * SOLUCIÓN APLICADA EN ESTE ARCHIVO:
 * - `restoreAuthFromSupabaseOnce()` se ejecuta UNA SOLA VEZ al arrancar el servidor (`bootRestored = true`),
 *   jamás durante el reinicio `515` del emparejamiento QR.
 * - En código `515 (restartRequired)`, reconecta en 150ms sin tocar el disco ni bloquear el handshake.
 * - `backupAuthToSupabase()` tiene *debounce* asíncrono (espera 4 segundos después de estabilizar la
 *   conexión para subir a Supabase en segundo plano sin trabar el emparejamiento).
 * - Apenas vincula (`connection === 'open'`) o al iniciar cualquier chat sin configurar, el bot
 *   envía automáticamente su presentación y pide el Contexto del Negocio, Menú y QR de cobro.
 */

import makeWASocket, {
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  Browsers,
  downloadMediaMessage
} from '@whiskeysockets/baileys';
import { createClient } from '@supabase/supabase-js';
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import http from 'node:http';
import pino from 'pino';

// ============================================================================
// ESCUDO ANTI-CRASH GLOBAL (PREVIENE ERROR 502 BAD GATEWAY EN RENDER)
// ============================================================================
process.on('uncaughtException', (err) => {
  console.error('🛡️ [Anti-Crash uncaughtException]:', err?.message || err);
});
process.on('unhandledRejection', (reason) => {
  console.error('🛡️ [Anti-Crash unhandledRejection]:', reason?.message || reason);
});

// ============================================================================
// 1. VARIABLES DE ENTORNO Y CONFIGURACIÓN
// ============================================================================
const PORT = Number(process.env.PORT || 3000);
const HOST = '0.0.0.0';
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://kjlhfagixbafscwfwbak.supabase.co';
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL = process.env.GEMINI_MODEL || 'gemini-3.5-flash-lite';
const RENDER_EXTERNAL_URL = process.env.RENDER_EXTERNAL_URL || 'https://wabot-qr-cloud.onrender.com';
const DEFAULT_TENANT_UUID = process.env.DEFAULT_TENANT_UUID || '41d1bc9f-ff83-4737-b357-e06e39c8a8df';
const AUTH_DIR = process.env.AUTH_DIR || './auth_info_baileys';

const WABOT_BILLING_INFO = {
  bank_name: process.env.WABOT_BANK_NAME || 'Banco Pichincha',
  account_type: process.env.WABOT_ACCOUNT_TYPE || 'Cuenta Transaccional',
  account_number: process.env.WABOT_ACCOUNT_NUMBER || '2214430178',
  beneficiary_name: process.env.WABOT_BENEFICIARY_NAME || 'Damian Fabricio Macancela Caguana',
  beneficiary_id: process.env.WABOT_BENEFICIARY_ID || '0107003048',
  superadmin_phone: (process.env.SUPERADMIN_PHONE || '593983476198').replace(/[^\d]/g, '')
};

const logger = pino({ level: 'silent' });
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false }
});

let latestQrData = null;
let latestPairingCode = null;
let connectionStatus = 'initializing';
let sockInstance = null;
let bootRestored = false; // Evita que restoreAuthFromSupabase pise el creds.json durante el código 515
let backupTimer = null;
let welcomeSentOnOpen = false;

let tenantCache = { data: null, items: [], updatedAt: 0 };
const onboardingState = new Map();
const greetedUsers = new Set();

function maskEnd(val = '', visible = 4) {
  const s = String(val);
  return s.length <= visible ? s : '*'.repeat(s.length - visible) + s.slice(-visible);
}

function normalizeText(str = '') {
  return String(str).normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().trim();
}

// ============================================================================
// 2. PERSISTENCIA NO BLOQUEANTE EN SUPABASE (CON DEBOUNCE PARA NO TRABAR EL QR)
// ============================================================================
function scheduleBackupAuthToSupabase(delayMs = 4000) {
  if (backupTimer) clearTimeout(backupTimer);
  backupTimer = setTimeout(async () => {
    try {
      if (!existsSync(join(AUTH_DIR, 'creds.json'))) return;
      const allFiles = readdirSync(AUTH_DIR).filter(f => f.endsWith('.json'));
      const files = {};
      for (const f of allFiles.slice(0, 70)) {
        files[f] = readFileSync(join(AUTH_DIR, f), 'utf-8');
      }
      await supabase.from('pending_uploads').upsert({
        tenant_id: DEFAULT_TENANT_UUID,
        phone: '__baileys_auth__',
        draft: { files, updated_at: new Date().toISOString() }
      }, { onConflict: 'tenant_id,phone' });
      console.log(`[Cloud Auth] Respaldo asíncrono completado (${Object.keys(files).length} archivos).`);
    } catch (err) {
      console.error('[Backup Auth Error]:', err.message);
    }
  }, delayMs);
}

async function restoreAuthFromSupabaseOnce() {
  if (bootRestored) return;
  bootRestored = true;
  if (!existsSync(AUTH_DIR)) mkdirSync(AUTH_DIR, { recursive: true });

  // Si ya hay un creds.json local válido en disco, no sobrescribirlo
  if (existsSync(join(AUTH_DIR, 'creds.json'))) return;

  try {
    const { data } = await supabase
      .from('pending_uploads')
      .select('draft')
      .eq('tenant_id', DEFAULT_TENANT_UUID)
      .eq('phone', '__baileys_auth__')
      .maybeSingle();

    const savedFiles = data?.draft?.files;
    if (!savedFiles || typeof savedFiles !== 'object' || !savedFiles['creds.json']) return;

    // Verificar que la sesión guardada realmente estuvo registrada (evita restaurar intentos fallidos a medias)
    const parsedCreds = JSON.parse(savedFiles['creds.json']);
    if (!parsedCreds?.me?.id) {
      console.warn('[Cloud Auth] Sesión en Supabase estaba incompleta (sin me.id). Se generará QR limpio.');
      return;
    }

    for (const [fileName, content] of Object.entries(savedFiles)) {
      writeFileSync(join(AUTH_DIR, fileName), content, 'utf-8');
    }
    console.log('[Cloud Auth] Sesión válida restaurada desde Supabase.');
  } catch (err) {
    console.error('[Restore Auth Error]:', err.message);
  }
}

async function wipeCorruptedSession() {
  console.warn('[Cloud Auth] Borrando sesión previa para generar un código QR 100% limpio...');
  if (backupTimer) clearTimeout(backupTimer);
  latestQrData = null;
  latestPairingCode = null;
  connectionStatus = 'initializing';
  welcomeSentOnOpen = false;
  try {
    if (sockInstance) {
      sockInstance.ev.removeAllListeners();
      sockInstance.ws?.close();
    }
  } catch {}
  try {
    if (existsSync(AUTH_DIR)) rmSync(AUTH_DIR, { recursive: true, force: true });
    mkdirSync(AUTH_DIR, { recursive: true });
  } catch {}
  try {
    await supabase
      .from('pending_uploads')
      .delete()
      .eq('tenant_id', DEFAULT_TENANT_UUID)
      .eq('phone', '__baileys_auth__');
  } catch {}
}

// ============================================================================
// 3. CACHÉ DEL COMERCIO Y DETECTOR DE CONTEXTO PENDIENTE AL INICIAR
// ============================================================================
async function getTenantBundle(forceRefresh = false) {
  if (!forceRefresh && tenantCache.data && Date.now() - tenantCache.updatedAt < 60000) {
    return tenantCache;
  }
  const [{ data: tenant }, { data: items }] = await Promise.all([
    supabase.from('merchants').select('*').eq('id', DEFAULT_TENANT_UUID).maybeSingle(),
    supabase.from('menu_items').select('*').eq('merchant_id', DEFAULT_TENANT_UUID).eq('available', true)
  ]);
  tenantCache = { data: tenant, items: items || [], updatedAt: Date.now() };
  return tenantCache;
}

function checkMissingSetupFields(tenant, items = []) {
  const ctx = tenant?.custom_context || {};
  const missing = [];
  if (!tenant?.name || tenant.name === 'Nuevo Comercio' || !ctx.giro || ctx.giro === 'Comercio General' || !ctx.contexto_configurado) {
    missing.push('contexto');
  }
  if (!Array.isArray(items) || items.length === 0) {
    missing.push('menu');
  }
  if (!ctx.datos_bancarios && !ctx.payment_qr_received) {
    missing.push('qr_cobro');
  }
  return missing;
}

// ============================================================================
// 4. SALUDO E INTRODUCCIÓN AUTÓNOMA QUE PIDE EL CONTEXTO AL INICIAR (0 TOKENS)
// ============================================================================
function buildBotSelfIntroductionPrompt(missingFields = ['contexto', 'menu', 'qr_cobro']) {
  const steps = [];
  if (missingFields.includes('contexto')) {
    steps.push(`1️⃣ *Contexto de tu negocio:* Nombre de tu local, a qué te dedicas (restaurante, tienda, clínica, etc.), horario, dirección y costo de envío a domicilio.`);
  }
  if (missingFields.includes('menu')) {
    steps.push(`2️⃣ *Tu Menú o Lista de Precios:* Escribe tus productos/servicios con precios o envíame una *FOTO / PDF* de tu carta.`);
  }
  if (missingFields.includes('qr_cobro')) {
    steps.push(`3️⃣ *Tu QR de Deuna! o Cuenta Bancaria + T&C:* Envíame la imagen de tu *QR de cobro* o tus datos bancarios (Banco, Cuenta, Titular y si aceptas Tarjeta de Crédito).`);
  }

  return (
    `👋 *¡Hola! Soy WaBot.AI 2079 EDGE, tu Asistente Comercial Autónomo con Inteligencia Artificial.*\n\n` +
    `🤖 *¿Qué soy y qué hago por ti?*\n` +
    `• Atiendo a tus clientes 24/7 en WhatsApp (en la nube, sin depender de tu PC).\n` +
    `• Entrego tus precios oficiales, tu Página Web y tu Menú Digital QR.\n` +
    `• Tomo pedidos o citas y valido comprobantes de pago *Deuna!* y transferencias con IA Vision.\n` +
    `• Opero bajo la *LOPDP Ecuador* (Arts. 10, 13 y 15: uso moderado y minimización de datos con purga automática a los 10 días).\n\n` +
    `📝 *Para iniciar, necesito que me ayudes con el contexto de tu negocio:*\n` +
    `${steps.join('\n')}\n\n` +
    `💡 *Envíame todo en un solo mensaje o mándame fotos de tu menú y tu QR de cobro aquí mismo.*`
  );
}

// ============================================================================
// 5. CLIENTE GEMINI DE BAJO CONSUMO DE TOKENS
// ============================================================================
async function gemini({ contents, systemInstruction, maxOutputTokens = 220, jsonMode = false }) {
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent?key=${GEMINI_API_KEY}`;
  const payload = {
    contents,
    generationConfig: {
      temperature: 0.1,
      maxOutputTokens,
      ...(jsonMode ? { responseMimeType: 'application/json' } : {})
    }
  };
  if (systemInstruction) {
    payload.systemInstruction = { parts: [{ text: systemInstruction }] };
  }
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload)
  });
  if (!res.ok) throw new Error(`Gemini HTTP ${res.status}: ${await res.text()}`);
  return res.json();
}

function extractTextFromParts(parts = []) {
  return parts.map(p => p.text || '').join('\n').trim();
}

function parseAIExtraction(raw = '') {
  const cleaned = raw.replace(/^```json\s*/i, '').replace(/^```\s*/i, '').replace(/```$/i, '').trim();
  try {
    return JSON.parse(cleaned);
  } catch {
    const match = cleaned.match(/\{[\s\S]*\}/);
    return match ? JSON.parse(match[0]) : {};
  }
}

// ============================================================================
// 6. FLUJO AUTÓNOMO DE CAPTURA DE CONTEXTO, MENÚ Y QR AL INICIAR
// ============================================================================
async function handleAutonomousSetup({ ctx, tenant, items, senderPhone, text, msg }) {
  const t = (text || '').trim();
  const tLower = t.toLowerCase();
  const missingBefore = checkMissingSetupFields(tenant, items);

  const isSetupKeyword = [
    'configurar', 'iniciar', 'contexto', 'cambiar contexto',
    'setup', 'cambiar menu', 'cambiar menú', 'onboarding'
  ].includes(tLower);

  // Si escribe comando de configurar, o si faltan datos del negocio y envía un saludo corto:
  // Primero se presenta y pide el contexto sin gastar ni 1 token de IA
  if (isSetupKeyword || (missingBefore.length > 0 && t.length < 20 && !msg?.message?.imageMessage && !msg?.message?.documentMessage)) {
    onboardingState.set(senderPhone, { stage: 'awaiting_setup' });
    await ctx.send(buildBotSelfIntroductionPrompt(missingBefore.length > 0 ? missingBefore : ['contexto', 'menu', 'qr_cobro']));
    return true;
  }

  // Si el negocio ya está 100% configurado y el usuario no pidió reconfigurar, pasar al flujo normal
  if (missingBefore.length === 0 && !onboardingState.has(senderPhone)) {
    return false;
  }

  let mediaPart = null;
  if (msg?.message?.imageMessage || msg?.message?.documentMessage) {
    const buf = await downloadMediaMessage(msg, 'buffer', {});
    const mimeType = msg.message.imageMessage?.mimetype || msg.message.documentMessage?.mimetype || 'image/jpeg';
    mediaPart = { inlineData: { mimeType, data: buf.toString('base64') } };
  }

  await ctx.send('🪄 *Guardando el contexto y catálogo de tu negocio...*');

  const extractPrompt = `Extrae del texto o imagen los datos comerciales para configurar el negocio. Devuelve SOLO JSON válido:
{
  "es_qr_pago": boolean,
  "nombre_negocio": string | null,
  "giro": string | null,
  "horario": string | null,
  "direccion": string | null,
  "costo_envio": number | null,
  "datos_bancarios": string | null,
  "titular_cuenta_ocr": string | null,
  "acepta_tc": boolean | null,
  "terminos_condiciones": string | null,
  "items": [{"nombre": string, "precio": number, "categoria": string}]
}
Mensaje: "${t}"`;

  const parts = [{ text: extractPrompt }];
  if (mediaPart) parts.push(mediaPart);

  try {
    const aiRes = await gemini({
      systemInstruction: sysPrompt,
      contents: [{ role: 'user', parts: [{ text }] }],
      maxOutputTokens: 200
    });
    const reply = extractTextFromParts(aiRes?.candidates?.[0]?.content?.parts);
    if (reply) await sendReply(reply);
  } catch (aiErr) {
    console.error('[Gemini AI Fallback]:', aiErr.message);
    await sendReply(`👋 ¡Hola! En este momento estoy recibiendo muchos pedidos. ¿Deseas consultar nuestro menú oficial? Escribe *menu* para ver la lista completa.`);
  }
}

// ============================================================================
// 9. MOTOR BAILEYS CON RECONEXIÓN INMEDIATA EN CÓDIGO 515 (NO SE QUEDA CONECTANDO)
// ============================================================================
async function startBot(forceWipe = false) {
  if (forceWipe) {
    await wipeCorruptedSession();
  } else {
    // Solo restaura desde Supabase una vez al iniciar el contenedor, jamás en medio del código 515
    await restoreAuthFromSupabaseOnce();
  }

  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    logger,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys, logger)
    },
    browser: Browsers.ubuntu('Chrome'),
    syncFullHistory: false,
    markOnlineOnConnect: true,
    generateHighQualityLinkPreview: false,
    connectTimeoutMs: 60000,
    defaultQueryTimeoutMs: 60000,
    keepAliveIntervalMs: 25000,
    getMessage: async () => undefined
  });

  sockInstance = sock;

  // CRÍTICO: Solo ejecutar saveCreds() en disco local durante el handshake;
  // el respaldo a Supabase se programa con debounce para no congelar "Conectando..." en el celular
  sock.ev.on('creds.update', async () => {
    await saveCreds();
    scheduleBackupAuthToSupabase(5000);
  });

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      latestQrData = qr;
      connectionStatus = 'qr_ready';
      console.log('[QR Ready] Nuevo QR disponible en /qr');
    }

    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      console.log(`[Connection Closed] statusCode=${statusCode}`);

      // CASO 1: Código 515 (restartRequired) ocurre SIEMPRE justo después de escanear el QR.
      // Debe reconectar en 150ms usando el creds.json local recién creado, SIN borrar ni sobrescribir nada.
      if (statusCode === DisconnectReason.restartRequired || statusCode === 515) {
        console.log('[Pairing Handshake 515] Reinicio rápido de emparejamiento en 150ms...');
        setTimeout(() => startBot(false), 150);
        return;
      }

      // CASO 2: Sesión cerrada desde el celular (401) o corrupta (500/403/405) -> Limpiar y dar QR nuevo
      if (
        statusCode === DisconnectReason.loggedOut ||
        statusCode === DisconnectReason.badSession ||
        statusCode === DisconnectReason.multideviceMismatch ||
        statusCode === 401 ||
        statusCode === 403 ||
        statusCode === 405
      ) {
        setTimeout(() => startBot(true), 1000);
        return;
      }

      // CASO 3: Desconexión de red normal -> Reconectar conservando credenciales locales
      connectionStatus = 'reconnecting';
      setTimeout(() => startBot(false), 2500);
    } else if (connection === 'open') {
      latestQrData = null;
      latestPairingCode = null;
      connectionStatus = 'connected';
      console.log('✅ [WaBot.AI 2079 EDGE] ¡WhatsApp conectado con éxito!');
      scheduleBackupAuthToSupabase(2000);

      // Apenas conecta por primera vez, el bot envía de forma autónoma el mensaje pidiendo el contexto del negocio
      if (!welcomeSentOnOpen) {
        welcomeSentOnOpen = true;
        setTimeout(async () => {
          try {
            const myJid = sock.user?.id?.replace(/:\d+@/, '@');
            const myPhone = (myJid || '').replace(/@.+$/, '').replace(/[^\d]/g, '');
            const { data: tenant, items } = await getTenantBundle(true);
            const missing = checkMissingSetupFields(tenant, items);
            const targetFields = missing.length > 0 ? missing : ['contexto', 'menu', 'qr_cobro'];

            if (myJid && sock && sock.user) {
              onboardingState.set(myPhone, { stage: 'awaiting_setup' });
              await sock.sendMessage(myJid, { text: buildBotSelfIntroductionPrompt(targetFields) });
              console.log('✅ [Welcome Sent] Prompt inicial enviado por WhatsApp con éxito.');
            }
          } catch (err) {
            console.error('[Welcome Prompt Error]:', err.message);
          }
        }, 3000);
      }
    }
  });

  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    if (type !== 'notify') return;
    for (const msg of messages) {
      try {
        if (!msg.message || msg.key.fromMe) continue;
        const remoteJid = msg.key.remoteJid || '';
        if (remoteJid.endsWith('@g.us') || remoteJid === 'status@broadcast') continue;

        const senderPhone = remoteJid.replace(/@.+$/, '').replace(/[^\d]/g, '');
        const text =
          msg.message.conversation ||
          msg.message.extendedTextMessage?.text ||
          msg.message.imageMessage?.caption ||
          msg.message.documentMessage?.caption ||
          '';

        await processIncomingMessage({
          senderPhone,
          text,
          msg,
          sendReply: async (replyText) => {
            try {
              if (sock && sock.user) {
                await sock.sendMessage(remoteJid, { text: replyText });
              }
            } catch (sendErr) {
              console.error('[Send Reply Error]:', sendErr.message);
            }
          }
        });
      } catch (loopErr) {
        console.error('[Message Loop Error]:', loopErr.message);
      }
    }
  });
}

// ============================================================================
// 10. SERVIDOR HTTP (/qr, /qr?reset=1, /pair Y /health)
// ============================================================================
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://127.0.0.1:${PORT}`);

  if (url.pathname === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({
      ok: true,
      connection_status: connectionStatus,
      whatsapp_connected: connectionStatus === 'connected',
      has_qr: Boolean(latestQrData),
      billing_bank: WABOT_BILLING_INFO.bank_name,
      billing_account_masked: maskEnd(WABOT_BILLING_INFO.account_number, 4),
      uptime_seconds: Math.floor(process.uptime()),
      user: sockInstance?.user?.id || null
    }));
    return;
  }

  if (url.pathname === '/api/qr-state') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({
      status: connectionStatus,
      qr: latestQrData,
      pairingCode: latestPairingCode,
      user: sockInstance?.user?.id || null
    }));
    return;
  }

  if (url.pathname === '/pair' && url.searchParams.get('phone')) {
    const phone = url.searchParams.get('phone').replace(/[^\d]/g, '');
    try {
      if (sockInstance && !sockInstance.authState?.creds?.registered) {
        latestPairingCode = await sockInstance.requestPairingCode(phone);
      }
    } catch (e) {
      console.error('[Pairing Code Error]:', e.message);
    }
    res.writeHead(302, { Location: '/qr' });
    res.end();
    return;
  }

  if (url.pathname === '/qr' && url.searchParams.get('reset') === '1') {
    await startBot(true);
    res.writeHead(302, { Location: '/qr' });
    res.end();
    return;
  }

  if (url.pathname === '/qr') {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    res.end(`<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>WaBot.AI 2079 EDGE — Vincular WhatsApp</title>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/qrcodejs/1.0.0/qrcode.min.js"></script>
  <style>
    body { font-family: system-ui, sans-serif; background: #0f172a; color: #f8fafc; text-align: center; padding: 24px; }
    .card { max-width: 480px; margin: 0 auto; background: #1e293b; padding: 26px; border-radius: 16px; box-shadow: 0 10px 30px rgba(0,0,0,0.5); }
    #qr-box { display: inline-block; background: #fff; padding: 16px; border-radius: 12px; margin: 16px auto; min-width: 256px; min-height: 256px; }
    .btn { display: inline-block; background: #ef4444; color: #fff; text-decoration: none; padding: 10px 18px; border-radius: 10px; font-weight: 700; margin-top: 12px; border: none; cursor: pointer; }
    .btn-pair { background: #38bdf8; color: #0f172a; }
    input { padding: 10px; border-radius: 8px; border: 1px solid #475569; width: 65%; margin-top: 10px; background: #0f172a; color: #f8fafc; }
    .code-badge { font-size: 26px; font-weight: 800; letter-spacing: 4px; color: #22c55e; background: #0f172a; padding: 12px; border-radius: 10px; margin: 12px 0; }
  </style>
</head>
<body>
  <div class="card">
    <h2>🤖 WaBot.AI 2079 EDGE — Panel QR</h2>
    <p id="status-text" style="color:#22c55e;font-weight:700;">Verificando estado...</p>
    <div id="qr-container" style="display:none;"><div id="qr-box"></div></div>
    <div id="pair-box" style="display:none;" class="code-badge"></div>

    <div style="margin-top:16px;border-top:1px solid #334155;padding-top:14px;">
      <a class="btn" href="/qr?reset=1">🔄 Borrar Sesión Atascada y Generar QR Nuevo</a>
    </div>

    <form action="/pair" method="GET" style="margin-top:16px;border-top:1px solid #334155;padding-top:14px;">
      <p style="font-size:13px;color:#cbd5e1;margin:4px 0;">O vincula con Código de 8 dígitos (pon tu número con 593):</p>
      <input type="text" name="phone" placeholder="Ej: 593983476198" required />
      <button type="submit" class="btn btn-pair">Obtener Código</button>
    </form>
  </div>
  <script>
    let currentQr = '';
    async function pollQr() {
      try {
        const res = await fetch('/api/qr-state');
        const data = await res.json();
        const qrContainer = document.getElementById('qr-container');
        const qrBox = document.getElementById('qr-box');
        const statusText = document.getElementById('status-text');
        const pairBox = document.getElementById('pair-box');

        if (data.pairingCode) {
          pairBox.style.display = 'block';
          pairBox.textContent = data.pairingCode;
        }
        if (data.status === 'connected') {
          statusText.textContent = '✅ ¡WhatsApp Conectado! Revisa tu chat de WhatsApp: el bot ya te pidió el contexto de tu negocio.';
          qrContainer.style.display = 'none';
          return;
        }
        if (data.qr) {
          statusText.textContent = '📲 Escanea este QR (Vinculará en 1 segundo):';
          qrContainer.style.display = 'block';
          if (data.qr !== currentQr) {
            currentQr = data.qr;
            qrBox.innerHTML = '';
            new QRCode(qrBox, { text: data.qr, width: 256, height: 256 });
          }
        } else {
          statusText.textContent = '⏳ Generando QR... Si se quedó pegado, toca el botón rojo de abajo.';
        }
      } catch (e) {}
    }
    pollQr();
    setInterval(pollQr, 2500);
  </script>
</body>
</html>`);
    return;
  }

  res.writeHead(302, { Location: '/qr' });
  res.end();
});

function startExternalKeepAlive() {
  if (!RENDER_EXTERNAL_URL) return;
  setInterval(async () => {
    try { await fetch(`${RENDER_EXTERNAL_URL}/health`); } catch {}
  }, 8 * 60 * 1000);
}

server.listen(PORT, HOST, () => {
  console.log(`[WaBot.AI 2079 EDGE] Servidor activo en puerto ${PORT}`);
  startExternalKeepAlive();
  startBot(false);
});
