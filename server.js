/**
 * WaBot.AI 2079 EDGE — wa-qr-server/server.js
 * SOLUCIÓN DEFINITIVA AL ERROR: "Couldn't link device / Check the phone number is correct"
 *
 * 1. VERSIÓN DE PROTOCOLO ACTUALIZADA (`getActiveWhatsAppVersion()`):
 *    Usa protocolo WhatsApp Web 2.3000.x ([2, 3000, 1019707846]), evitando que WhatsApp rechace
 *    el vínculo con "Couldn't link device" por versiones 2.24xx obsoletas.
 * 2. SOCKET ENVEJECIDO AL PEDIR EL CÓDIGO DE 8 DÍGITOS (`/pair`):
 *    Al llamar `/pair`, inicia un socket fresco y pide el código en el primer evento QR del socket.
 * 3. NORMALIZACIÓN DE TELÉFONO ECUATORIANO (`normalizeEcuadorPhone`):
 *    Convierte cualquier formato (0983476198, 5930983476198, 983476198) a 5939XXXXXXXX exacto.
 * 4. FIRMA DE NAVEGADOR EXIGIDA POR requestPairingCode:
 *    Usa `browser: ['Ubuntu', 'Chrome', '20.0.04']` y `markOnlineOnConnect: false`.
 * 5. CASCADA DE MODELOS GEMINI:
 *    gemini-2.5-flash -> gemini-2.5-flash-lite -> gemini-2.0-flash -> gemini-3.5-flash-lite.
 * 6. ONBOARDING FORZADO:
 *    Pide siempre el contexto del negocio, catálogo y QR de cobro al iniciar.
 */

import makeWASocket, {
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
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
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
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
let pairingTargetPhone = null;
let connectionStatus = 'initializing';
let lastDisconnectCode = null;
let sockInstance = null;
let isStartingSocket = false;
let bootRestored = false;
let backupTimer = null;

let tenantCache = { data: null, items: [], updatedAt: 0 };
const onboardingState = new Map();
const greetedUsers = new Set();

// Normalizador estricto de números de Ecuador para evitar "Check the phone number is correct"
export function normalizeEcuadorPhone(raw = '') {
  let digits = String(raw).replace(/[^\d]/g, '');
  // Si empieza con 09 (ej. 0983476198 -> 593983476198)
  if (digits.length === 10 && digits.startsWith('09')) {
    return '593' + digits.slice(1);
  }
  // Si pusieron 59309... (ej. 5930983476198 -> 593983476198)
  if (digits.length === 13 && digits.startsWith('59309')) {
    return '593' + digits.slice(4);
  }
  // Si pusieron solo los 9 dígitos empezando con 9 (ej. 983476198 -> 593983476198)
  if (digits.length === 9 && digits.startsWith('9')) {
    return '593' + digits;
  }
  return digits;
}

// Obtiene la versión activa real de WhatsApp Web 2.3000.x (evita el fallo de versión 2.2413 obsoleta)
async function getActiveWhatsAppVersion() {
  try {
    const res = await fetch('https://web.whatsapp.com/check-update?version=2.3000.1019707846&platform=web', {
      headers: { 'user-agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0' }
    });
    if (res.ok) {
      const data = await res.json();
      if (data?.currentVersion) {
        const parts = data.currentVersion.split('.').map(Number);
        if (parts.length === 3 && parts[0] >= 2 && parts[1] >= 3000) {
          return parts;
        }
      }
    }
  } catch {}
  try {
    const { version } = await fetchLatestBaileysVersion();
    if (Array.isArray(version) && version[1] >= 3000) return version;
  } catch {}
  // Fallback garantizado v2.3000.x exigido por los servidores actuales de Meta
  return [2, 3000, 1019707846];
}

function normalizeText(str = '') {
  return String(str).normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().trim();
}

function parseCustomContext(rawCtx) {
  if (!rawCtx) return {};
  if (typeof rawCtx === 'object') return rawCtx;
  try { return JSON.parse(rawCtx); } catch { return {}; }
}

// ============================================================================
// 2. RESPALDO Y LIMPIEZA DE SESIÓN EN SUPABASE
// ============================================================================
function scheduleBackupAuthToSupabase(delayMs = 5000) {
  if (backupTimer) clearTimeout(backupTimer);
  backupTimer = setTimeout(async () => {
    try {
      const credsFile = join(AUTH_DIR, 'creds.json');
      if (!existsSync(credsFile)) return;
      const parsedCreds = JSON.parse(readFileSync(credsFile, 'utf-8'));
      if (!parsedCreds?.me?.id) return;

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
    } catch (err) {
      console.error('[Backup Auth Error]:', err.message);
    }
  }, delayMs);
}

async function restoreAuthFromSupabaseOnce() {
  if (bootRestored) return;
  bootRestored = true;
  if (!existsSync(AUTH_DIR)) mkdirSync(AUTH_DIR, { recursive: true });
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

    const parsedCreds = JSON.parse(savedFiles['creds.json']);
    if (!parsedCreds?.me?.id) return;

    for (const [fileName, content] of Object.entries(savedFiles)) {
      writeFileSync(join(AUTH_DIR, fileName), content, 'utf-8');
    }
    console.log('[Cloud Auth] Sesión válida restaurada:', parsedCreds.me.id);
  } catch {}
}

async function wipeCorruptedSessionAndResetOnboarding(preservePairingTarget = false) {
  if (backupTimer) clearTimeout(backupTimer);
  latestQrData = null;
  if (!preservePairingTarget) {
    latestPairingCode = null;
    pairingTargetPhone = null;
  }
  connectionStatus = 'initializing';
  try {
    if (sockInstance) {
      sockInstance.ev.removeAllListeners();
      sockInstance.ws?.close();
      sockInstance = null;
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

    const { data: merchant } = await supabase.from('merchants').select('custom_context').eq('id', DEFAULT_TENANT_UUID).maybeSingle();
    const ctx = parseCustomContext(merchant?.custom_context);
    ctx.onboarding_done_v2 = false;
    await supabase.from('merchants').update({ custom_context: ctx }).eq('id', DEFAULT_TENANT_UUID);
    tenantCache.updatedAt = 0;
  } catch {}
}

// ============================================================================
// 3. CLIENTE GEMINI CON CASCADA AUTOMÁTICA DE MODELOS
// ============================================================================
const GEMINI_MODELS = [
  process.env.GEMINI_MODEL || 'gemini-2.5-flash',
  'gemini-2.5-flash-lite',
  'gemini-2.0-flash',
  'gemini-3.5-flash-lite',
  'gemini-1.5-flash'
];

async function gemini({ contents, systemInstruction, maxOutputTokens = 220, jsonMode = false }) {
  let lastError = null;
  for (const modelName of GEMINI_MODELS) {
    try {
      const url = `https://generativelanguage.googleapis.com/v1beta/models/${modelName}:generateContent?key=${GEMINI_API_KEY}`;
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
      if (res.status === 404) continue;
      if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);
      return await res.json();
    } catch (err) {
      lastError = err;
    }
  }
  throw lastError || new Error('Gemini no disponible');
}

function extractTextFromParts(parts = []) {
  return parts.map(p => p.text || '').join('\n').trim();
}

function parseAIExtraction(raw = '') {
  const cleaned = raw.replace(/^```json\s*/i, '').replace(/^```\s*/i, '').replace(/```$/i, '').trim();
  try { return JSON.parse(cleaned); } catch {
    const m = cleaned.match(/\{[\s\S]*\}/);
    return m ? JSON.parse(m[0]) : {};
  }
}

// ============================================================================
// 4. SALUDO E INTRODUCCIÓN QUE PIDE EL CONTEXTO AL INICIAR
// ============================================================================
async function getTenantBundle(forceRefresh = false) {
  if (!forceRefresh && tenantCache.data && Date.now() - tenantCache.updatedAt < 45000) {
    return tenantCache;
  }
  const [{ data: tenant }, { data: items }] = await Promise.all([
    supabase.from('merchants').select('*').eq('id', DEFAULT_TENANT_UUID).maybeSingle(),
    supabase.from('menu_items').select('*').eq('merchant_id', DEFAULT_TENANT_UUID).eq('available', true)
  ]);
  if (tenant) tenant.custom_context = parseCustomContext(tenant.custom_context);
  tenantCache = { data: tenant, items: items || [], updatedAt: Date.now() };
  return tenantCache;
}

function buildBotSelfIntroductionPrompt() {
  return (
    `👋 *¡Hola! Soy WaBot.AI 2079 EDGE, tu Asistente Comercial Autónomo con Inteligencia Artificial.*\n\n` +
    `🤖 *¿Qué hago y para qué sirvo?*\n` +
    `• Atiendo a tus clientes 24/7 en WhatsApp, muestro tu menú digital, tomo pedidos o citas y valido pagos de *Deuna!* y transferencias con IA.\n` +
    `• Cumplo con la *LOPDP Ecuador* (Arts. 10, 13 y 15: uso moderado de datos con purga automática a los 10 días).\n\n` +
    `📝 *Ayúdame con el contexto de tu negocio para empezar. Envíame ahora:*\n` +
    `1️⃣ *Contexto de tu negocio:* Nombre de tu local, qué vendes, dirección, horario y costo de envío.\n` +
    `2️⃣ *Tu Menú o Precios:* Lista en texto o *FOTO / PDF* de tus productos y precios.\n` +
    `3️⃣ *Tu QR de Deuna! o Cuenta Bancaria:* Imagen de tu *QR de cobro* o número de cuenta y titular.`
  );
}

async function handleAutonomousSetup({ ctx, tenant, items, senderPhone, text, msg }) {
  const t = (text || '').trim();
  const tLower = normalizeText(t);
  const cCtx = parseCustomContext(tenant.custom_context);

  const isSetupCmd = ['configurar', 'iniciar', 'contexto', 'cambiar contexto', 'setup', 'cambiar menu'].includes(tLower);
  const needsOnboarding = !cCtx.onboarding_done_v2 || items.length === 0;
  const inFlow = onboardingState.has(senderPhone);

  if (isSetupCmd || (needsOnboarding && !inFlow && t.length < 25 && !msg?.message?.imageMessage && !msg?.message?.documentMessage)) {
    onboardingState.set(senderPhone, { step: 'waiting_business_data' });
    await ctx.send(buildBotSelfIntroductionPrompt());
    return true;
  }

  if (!needsOnboarding && !inFlow) return false;

  let mediaPart = null;
  if (msg?.message?.imageMessage || msg?.message?.documentMessage) {
    try {
      const buf = await downloadMediaMessage(msg, 'buffer', {});
      const mimeType = msg.message.imageMessage?.mimetype || msg.message.documentMessage?.mimetype || 'image/jpeg';
      mediaPart = { inlineData: { mimeType, data: buf.toString('base64') } };
    } catch (e) {
      console.error('[Media Error]:', e.message);
    }
  }

  await ctx.send('🪄 *Configurando el contexto y catálogo de tu negocio...*');

  const extractPrompt = `Extrae del mensaje o imagen los datos del negocio en JSON válido:
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

  let ext = {};
  try {
    const aiRes = await gemini({ contents: [{ role: 'user', parts }], maxOutputTokens: 800, jsonMode: true });
    ext = parseAIExtraction(extractTextFromParts(aiRes?.candidates?.[0]?.content?.parts));
  } catch (err) {
    console.error('[Setup Error]:', err.message);
  }

  const updatedCtx = { ...cCtx, onboarding_done_v2: true };
  const updateMerchant = {};

  if (ext.nombre_negocio) updateMerchant.name = ext.nombre_negocio;
  if (ext.direccion) updateMerchant.address = ext.direccion;
  if (ext.horario) {
    const [open, close] = String(ext.horario).split('-').map(s => s.trim());
    if (open) updateMerchant.open_time = open;
    if (close) updateMerchant.close_time = close;
  }
  if (ext.giro) updatedCtx.giro = ext.giro;
  if (ext.costo_envio !== null && ext.costo_envio !== undefined) updatedCtx.costo_envio = Number(ext.costo_envio) || 0;
  if (ext.datos_bancarios) updatedCtx.datos_bancarios = ext.datos_bancarios;
  if (ext.titular_cuenta_ocr) updatedCtx.titular_cuenta_ocr = ext.titular_cuenta_ocr;
  if (ext.acepta_tc !== null && ext.acepta_tc !== undefined) updatedCtx.acepta_tc = Boolean(ext.acepta_tc);
  if (ext.terminos_condiciones) updatedCtx.terminos_condiciones = ext.terminos_condiciones;
  if (ext.es_qr_pago) updatedCtx.payment_qr_received = true;

  updateMerchant.custom_context = updatedCtx;
  await supabase.from('merchants').update(updateMerchant).eq('id', tenant.id);

  if (Array.isArray(ext.items) && ext.items.length > 0) {
    await supabase.from('menu_items').delete().eq('merchant_id', tenant.id);
    const rows = ext.items.map(i => ({
      merchant_id: tenant.id,
      name: String(i.nombre || 'Producto').slice(0, 100),
      price: Number(i.precio) || 0,
      category: String(i.categoria || 'General').slice(0, 50),
      available: true
    }));
    await supabase.from('menu_items').insert(rows);
  }

  onboardingState.delete(senderPhone);
  const refreshed = await getTenantBundle(true);
  await ctx.send(
    `🎉 *¡Tu Negocio "${refreshed.data.name}" está Configurado y Activo!*\n\n` +
    `• *Giro:* ${updatedCtx.giro || 'Comercio'}\n` +
    `• *Productos cargados:* ${refreshed.items.length}\n` +
    `• *Pagos:* ${updatedCtx.datos_bancarios || 'Configurado'}\n\n` +
    `Escribe *configurar* cuando desees actualizar tu contexto o menú.`
  );
  return true;
}

// ============================================================================
// 5. ENRUTADOR DE 0 TOKENS Y PROCESADOR DE MENSAJES
// ============================================================================
function tryZeroTokenCustomerReply(tenant, items, text, isFirstMessage) {
  const clean = normalizeText(text);
  const ctx = parseCustomContext(tenant.custom_context);
  const isAllInOne = tenant.plan === 'all_in_one' || tenant.plan === 'trial';
  const webUrl = tenant.website_url || `https://wabot.ec/b/${tenant.slug}`;
  const menuUrl = tenant.menu_url || `https://wabot.ec/b/${tenant.slug}/menu`;

  const intro = isFirstMessage
    ? `👋 ¡Hola! Soy el Asistente Virtual de *${tenant.name}* (${ctx.giro || 'Comercio'}), impulsado por *WaBot.AI 2079 EDGE*.\n` +
      `🔒 *LOPDP Ecuador:* Uso moderado de datos (retención 10 días; escribe *mis datos* o *borrar mis datos*).\n\n`
    : '';

  if (/^(hola|buenas|buenos dias|buenas tardes|buenas noches|info|informacion)$/.test(clean)) {
    const sample = items.slice(0, 6).map(i => `• *${i.name}:* $${Number(i.price).toFixed(2)}`).join('\n');
    return (
      `${intro || `👋 ¡Bienvenido a *${tenant.name}*!\n\n`}` +
      `🕒 *Horario:* ${tenant.open_time || '09:00'} - ${tenant.close_time || '20:00'} | 📍 ${tenant.address || tenant.city}\n\n` +
      `📋 *Catálogo:*\n${sample}\n\n` +
      (isAllInOne ? `🍽️ *Menú Digital QR:* ${menuUrl}\n🌐 *Web:* ${webUrl}\n\n` : '') +
      `¿Qué deseas pedir hoy? *(Escribe **configurar** si deseas actualizar el contexto del negocio)*`
    );
  }

  if (/^(menu|catalogo|precios|lista de precios|carta|que venden)$/.test(clean)) {
    const list = items.map(i => `• *${i.name}:* $${Number(i.price).toFixed(2)}`).join('\n');
    return `${intro}📖 *MENÚ OFICIAL — ${tenant.name}*\n\n${list}\n\n🚚 *Envío:* $${Number(ctx.costo_envio || 0).toFixed(2)}`;
  }

  if (/^(cuenta|deuna|transferencia|qr|datos bancarios|como pago|metodos de pago)$/.test(clean)) {
    return `${intro}💳 *DATOS DE PAGO — ${tenant.name}*\n\n• *Cuenta / Deuna!:* ${ctx.datos_bancarios || 'Confirmar al cerrar pedido'}\n• *Titular:* ${ctx.titular_cuenta_ocr || tenant.name}`;
  }

  return null;
}

async function processIncomingMessage({ senderPhone, text, msg, sendReply }) {
  const { data: tenant, items } = await getTenantBundle();
  if (!tenant) return;

  const cleanLower = (text || '').trim().toLowerCase();
  if (cleanLower === 'mis datos' || cleanLower === 'consultar mis datos') {
    const { data: arco } = await supabase.rpc('export_customer_data', { p_tenant: tenant.id, p_phone: senderPhone });
    await sendReply(`📄 *REPORTE LOPDP ECUADOR (Art. 13)*\n• *Teléfono:* +${senderPhone}\n• *Pedidos:* ${arco?.pedidos?.length || 0}\nEscribe *borrar mis datos* para anonimizar.`);
    return;
  }
  if (cleanLower === 'borrar mis datos' || cleanLower === 'eliminar mis datos') {
    await supabase.rpc('anonymize_customer', { p_tenant: tenant.id, p_phone: senderPhone });
    await sendReply(`🔒 *Datos anonimizados irreversiblemente (Art. 15 LOPDP Ecuador).*`);
    return;
  }

  const handledBySetup = await handleAutonomousSetup({ ctx: { send: sendReply }, tenant, items, senderPhone, text, msg });
  if (handledBySetup) return;

  const isFirstMsg = !greetedUsers.has(senderPhone);
  greetedUsers.add(senderPhone);

  const zeroReply = tryZeroTokenCustomerReply(tenant, items, text, isFirstMsg);
  if (zeroReply) {
    await sendReply(zeroReply);
    return;
  }

  const ctx = parseCustomContext(tenant.custom_context);
  const compactCatalog = items.map(i => `${i.name}:$${Number(i.price).toFixed(2)}`).join(' | ');
  const sysPrompt = `Asistente de "${tenant.name}" (${ctx.giro || 'Comercio'}). Catálogo USD: ${compactCatalog}. Envío: $${Number(ctx.costo_envio || 0).toFixed(2)}. Pagos: ${ctx.datos_bancarios || 'Deuna/Transferencia'}. Sé breve (máximo 4 líneas) y no inventes precios.`;

  try {
    const aiRes = await gemini({
      systemInstruction: sysPrompt,
      contents: [{ role: 'user', parts: [{ text }] }],
      maxOutputTokens: 200
    });
    const reply = extractTextFromParts(aiRes?.candidates?.[0]?.content?.parts);
    if (reply) {
      await sendReply(reply);
      return;
    }
  } catch (err) {
    console.error('[Gemini AI Error]:', err.message);
  }

  await sendReply(
    `👋 ¡Hola! En *${tenant.name}* estamos atentos a tus pedidos. Escribe *menu* para ver la lista de precios oficial o *cuenta* para formas de pago.`
  );
}

// ============================================================================
// 6. MOTOR BAILEYS CON VERSIÓN 2.3000.x Y PAIRING CODE EN SOCKET FRESCO
// ============================================================================
async function startBot(forceWipe = false, phoneForPairingCode = null) {
  if (isStartingSocket) return;
  isStartingSocket = true;

  try {
    if (phoneForPairingCode) {
      pairingTargetPhone = normalizeEcuadorPhone(phoneForPairingCode);
      latestPairingCode = null;
      await wipeCorruptedSessionAndResetOnboarding(true);
    } else if (forceWipe) {
      await wipeCorruptedSessionAndResetOnboarding(false);
    } else {
      await restoreAuthFromSupabaseOnce();
    }

    const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
    const version = await getActiveWhatsAppVersion();
    console.log(`[Baileys] Usando protocolo WhatsApp Web v${version.join('.')}`);

    const sock = makeWASocket({
      version,
      logger,
      printQRInTerminal: false,
      auth: {
        creds: state.creds,
        keys: makeCacheableSignalKeyStore(state.keys, logger)
      },
      // IMPORTANTE: Tupla exacta requerida por WhatsApp para que requestPairingCode y QR NO den "Couldn't link device"
      browser: ['Ubuntu', 'Chrome', '20.0.04'],
      syncFullHistory: false,
      markOnlineOnConnect: false,
      generateHighQualityLinkPreview: false,
      connectTimeoutMs: 60000,
      defaultQueryTimeoutMs: 60000,
      keepAliveIntervalMs: 25000,
      getMessage: async () => undefined
    });

    sockInstance = sock;

    sock.ev.on('creds.update', async () => {
      await saveCreds();
      scheduleBackupAuthToSupabase(5000);
    });

    sock.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (typeof qr === 'string' && qr.length > 10) {
        latestQrData = qr;
        connectionStatus = 'qr_ready';

        // Si el usuario solicitó vincular con Código de 8 dígitos, pedirlo en este primer evento QR del socket fresco
        if (pairingTargetPhone && !latestPairingCode && !sock.authState?.creds?.registered) {
          const targetPhone = pairingTargetPhone;
          pairingTargetPhone = null; // Pedirlo una sola vez para no invalidar el código anterior
          setTimeout(async () => {
            try {
              const rawCode = await sock.requestPairingCode(targetPhone);
              latestPairingCode = rawCode?.match(/.{1,4}/g)?.join('-') || rawCode;
              console.log(`[Pairing Code Generado] Para +${targetPhone}: ${latestPairingCode}`);
            } catch (err) {
              console.error('[Error requestPairingCode]:', err.message);
            }
          }, 500);
        }
      }

      if (connection === 'close') {
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        lastDisconnectCode = statusCode;
        console.warn(`[Socket Closed] statusCode=${statusCode}`);

        // Reinicio 515 tras escanear QR o ingresar código de 8 dígitos: reconectar de inmediato sin borrar nada
        if (statusCode === DisconnectReason.restartRequired || statusCode === 515) {
          setTimeout(() => startBot(false, null), 150);
          return;
        }

        if (statusCode === DisconnectReason.connectionReplaced || statusCode === 440) {
          connectionStatus = 'conflict_other_instance';
          setTimeout(() => startBot(false, null), 10000);
          return;
        }

        if (
          statusCode === DisconnectReason.loggedOut ||
          statusCode === DisconnectReason.badSession ||
          statusCode === DisconnectReason.multideviceMismatch ||
          statusCode === 401 ||
          statusCode === 403 ||
          statusCode === 500
        ) {
          setTimeout(() => startBot(true, null), 1200);
          return;
        }

        connectionStatus = 'reconnecting';
        setTimeout(() => startBot(false, null), 3000);
      } else if (connection === 'open') {
        latestQrData = null;
        latestPairingCode = null;
        pairingTargetPhone = null;
        connectionStatus = 'connected';
        scheduleBackupAuthToSupabase(2000);

        try {
          const myJid = sock.user?.id?.replace(/:\d+@/, '@');
          const myPhone = (myJid || '').replace(/@.+$/, '').replace(/[^\d]/g, '');
          if (myJid) {
            onboardingState.set(myPhone, { step: 'waiting_business_data' });
            await sock.sendMessage(myJid, { text: buildBotSelfIntroductionPrompt() });
          }
        } catch (err) {
          console.error('[Welcome Send Error]:', err.message);
        }
      }
    });

    sock.ev.on('messages.upsert', async ({ messages, type }) => {
      if (type !== 'notify') return;
      for (const msg of messages) {
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
            } catch (e) {
              console.error('[Send Reply Error]:', e.message);
            }
          }
        });
      }
    });
  } finally {
    isStartingSocket = false;
  }
}

// ============================================================================
// 7. SERVIDOR HTTP (/qr, /pair CON SOCKET FRESCO + FORMATO 593 AUTOMÁTICO)
// ============================================================================
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://127.0.0.1:${PORT}`);

  if (url.pathname === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({
      ok: true,
      connection_status: connectionStatus,
      last_disconnect_code: lastDisconnectCode,
      has_qr: Boolean(latestQrData),
      pairing_code: latestPairingCode,
      user: sockInstance?.user?.id || null
    }));
    return;
  }

  if (url.pathname === '/api/qr-state') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({
      status: connectionStatus,
      lastDisconnectCode,
      qr: latestQrData,
      pairingCode: latestPairingCode,
      user: sockInstance?.user?.id || null
    }));
    return;
  }

  // Vincular con código de 8 dígitos sobre un socket recién creado y con teléfono 593 normalizado
  if (url.pathname === '/pair' && url.searchParams.get('phone')) {
    const cleanPhone = normalizeEcuadorPhone(url.searchParams.get('phone'));
    await startBot(true, cleanPhone);
    res.writeHead(302, { Location: '/qr' });
    res.end();
    return;
  }

  if (url.pathname === '/qr' && url.searchParams.get('reset') === '1') {
    await startBot(true, null);
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
    body { font-family: system-ui, sans-serif; background: #0f172a; color: #f8fafc; text-align: center; padding: 20px; }
    .card { max-width: 500px; margin: 0 auto; background: #1e293b; padding: 24px; border-radius: 16px; }
    #qr-box { display: inline-block; background: #ffffff; padding: 18px; border-radius: 12px; margin: 14px auto; }
    .btn { display: inline-block; background: #ef4444; color: #fff; text-decoration: none; padding: 11px 18px; border-radius: 10px; font-weight: 700; margin-top: 10px; border: none; cursor: pointer; }
    .btn-pair { background: #22c55e; color: #052e16; width: 100%; font-size: 15px; }
    input { padding: 12px; border-radius: 8px; border: 1px solid #475569; width: 90%; margin-top: 8px; font-size: 16px; text-align: center; }
    .code-badge { font-size: 30px; font-weight: 800; letter-spacing: 5px; color: #22c55e; background: #0f172a; border: 2px solid #22c55e; padding: 16px; border-radius: 12px; margin: 14px 0; }
  </style>
</head>
<body>
  <div class="card">
    <h2>🤖 WaBot.AI 2079 EDGE — Vinculación Oficial</h2>
    <p id="status-text" style="color:#22c55e;font-weight:700;">Verificando conexión...</p>

    <div id="pair-container" style="display:none;">
      <p style="color:#38bdf8;font-weight:700;margin-bottom:4px;">📲 Ingresa este código en tu WhatsApp ahora:</p>
      <div id="pair-box" class="code-badge"></div>
    </div>

    <div id="qr-container" style="display:none;">
      <div id="qr-box"></div>
    </div>

    <form action="/pair" method="GET" style="margin-top:14px;border-top:1px solid #334155;padding-top:14px;">
      <p style="font-size:14px;color:#f8fafc;font-weight:700;margin:4px 0;">🔑 Vincular con Número de Teléfono (Código de 8 dígitos):</p>
      <p style="font-size:12px;color:#94a3b8;margin:4px 0;">Puedes escribir 0983476198 o 593983476198 (se convierte automáticamente a 593 sin el 0):</p>
      <input type="text" name="phone" placeholder="0983476198" required />
      <button type="submit" class="btn btn-pair">Generar Código Nuevo de 8 Dígitos</button>
    </form>

    <div style="margin-top:14px;border-top:1px solid #334155;padding-top:14px;">
      <a class="btn" href="/qr?reset=1">🔄 Limpiar Sesión y Generar Nuevo QR</a>
    </div>
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
        const pairContainer = document.getElementById('pair-container');
        const pairBox = document.getElementById('pair-box');

        if (data.status === 'connected') {
          statusText.textContent = '✅ ¡WhatsApp Vinculado con Éxito! Revisa tu WhatsApp: el bot ya te pidió el contexto de tu negocio.';
          qrContainer.style.display = 'none';
          pairContainer.style.display = 'none';
          return;
        }

        if (data.pairingCode) {
          pairContainer.style.display = 'block';
          pairBox.textContent = data.pairingCode;
          statusText.textContent = '✅ Código listo. En tu celular ve a Dispositivos vinculados > Vincular con el número de teléfono:';
          qrContainer.style.display = 'none';
          return;
        }

        if (data.qr) {
          statusText.textContent = '📲 Escanea este QR o pide abajo un código de 8 dígitos:';
          qrContainer.style.display = 'block';
          if (data.qr !== currentQr) {
            currentQr = data.qr;
            qrBox.innerHTML = '';
            try {
              new QRCode(qrBox, { text: data.qr, width: 256, height: 256, correctLevel: QRCode.CorrectLevel.L });
            } catch (err) {
              qrBox.innerHTML = '<img src="https://api.qrserver.com/v1/create-qr-code/?size=256x256&ecc=L&margin=10&data=' + encodeURIComponent(data.qr) + '" width="256" height="256" />';
            }
          }
        }
      } catch (e) {}
    }
    pollQr();
    setInterval(pollQr, 2000);
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
  startBot(false, null);
});
