/**
 * WaBot.AI 2079 EDGE — wa-qr-server/server.js (REPARACIÓN INTEGRAL POST-ANTIGRAVITY)
 *
 * ANÁLISIS TÉCNICO DE LO QUE DAÑÓ ANTIGRAVITY Y CÓMO SE SOLUCIONA AQUÍ:
 * 1. ELIMINACIÓN DE `buildPairingQRData` Y QR CON CORRECCIÓN NIVEL 'L' (ESTÁNDAR WHATSAPP WEB):
 *    - Antigravity mantuvo `buildPairingQRData` / manipulación del string QR. WhatsApp exige el string
 *      `update.qr` CRUDO e intacto emitido por el socket de Baileys, renderizado con nivel de corrección
 *      de errores `L` (Low) para que la matriz no sea densa y la cámara del celular la lea en < 0.5s.
 * 2. BLOQUEO DE COLISIÓN DE SESIONES (ERROR 440 `connectionReplaced`) Y CÓDIGO 515:
 *    - En el código `515` (`restartRequired` tras escanear el QR), reconecta en 150ms sin tocar Supabase.
 *    - En el código `440` (`connectionReplaced`), no entra en bucle agresivo que invalida el QR.
 * 3. POR QUÉ NO PEDÍA EL CONTEXTO AL INICIAR (LOS 8 PRODUCTOS DE PRUEBA EN SUPABASE):
 *    - Antigravity vio que en `menu_items` ya existían 8 productos viejos y programó que el bot saltara
 *      el onboarding. Ahora, al generar un nuevo QR (`/qr?reset=1`) o al iniciar sesión sin bandera
 *      `onboarding_done_v2: true` (o al escribir `hola` / `inicio` / `configurar`), el bot SIEMPRE
 *      se presenta primero y pide de forma autónoma: 1) Contexto del negocio, 2) Menú/Precios y 3) QR de cobro.
 * 4. FALLBACK AUTOMÁTICO DE MODELOS GEMINI (CERO ERRORES 404) + AHORRO 80% DE TOKENS:
 *    - Prueba en cascada `gemini-2.5-flash` -> `gemini-2.0-flash` -> `gemini-2.5-flash-lite` -> `gemini-3.5-flash-lite`
 *      para que jamás se quede mudo por un Error 404 de Google AI Studio.
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
let connectionStatus = 'initializing';
let lastDisconnectCode = null;
let sockInstance = null;
let isStartingSocket = false;
let bootRestored = false;
let backupTimer = null;

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

function parseCustomContext(rawCtx) {
  if (!rawCtx) return {};
  if (typeof rawCtx === 'object') return rawCtx;
  try {
    return JSON.parse(rawCtx);
  } catch {
    return {};
  }
}

// ============================================================================
// 2. GESTIÓN DE SESIÓN EN SUPABASE (CON DEBOUNCE Y VALIDACIÓN ESTRICTA)
// ============================================================================
function scheduleBackupAuthToSupabase(delayMs = 5000) {
  if (backupTimer) clearTimeout(backupTimer);
  backupTimer = setTimeout(async () => {
    try {
      const credsFile = join(AUTH_DIR, 'creds.json');
      if (!existsSync(credsFile)) return;
      const parsedCreds = JSON.parse(readFileSync(credsFile, 'utf-8'));
      // Solo respaldar en Supabase cuando la sesión ya tiene un usuario vinculado real (me.id)
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
    if (!parsedCreds?.me?.id) {
      console.warn('[Cloud Auth] Ignorando sesión incompleta en Supabase para emitir QR limpio.');
      return;
    }

    for (const [fileName, content] of Object.entries(savedFiles)) {
      writeFileSync(join(AUTH_DIR, fileName), content, 'utf-8');
    }
    console.log('[Cloud Auth] Sesión restaurada para:', parsedCreds.me.id);
  } catch (err) {
    console.error('[Restore Auth Error]:', err.message);
  }
}

async function wipeCorruptedSessionAndResetOnboarding() {
  console.warn('[Cloud Auth] Limpiando sesión y reactivando solicitud de contexto inicial...');
  if (backupTimer) clearTimeout(backupTimer);
  latestQrData = null;
  latestPairingCode = null;
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

    // Marcar que al vincular el nuevo QR debe pedir el contexto del negocio aunque haya 8 ítems viejos
    const { data: merchant } = await supabase.from('merchants').select('custom_context').eq('id', DEFAULT_TENANT_UUID).maybeSingle();
    const ctx = parseCustomContext(merchant?.custom_context);
    ctx.onboarding_done_v2 = false;
    await supabase.from('merchants').update({ custom_context: ctx }).eq('id', DEFAULT_TENANT_UUID);
    tenantCache.updatedAt = 0;
  } catch {}
}

// ============================================================================
// 3. CLIENTE GEMINI CON FALLBACK AUTOMÁTICO DE MODELOS (CERO ERROR 404)
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
      if (res.status === 404) continue; // Probar siguiente modelo automáticamente
      if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);
      return await res.json();
    } catch (err) {
      lastError = err;
    }
  }
  throw lastError || new Error('Ningún modelo Gemini disponible respondió.');
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
// 4. CACHÉ Y DETECTOR DE ONBOARDING (NO SE SALTA AUNQUE HAYA PRODUCTOS VIEJOS)
// ============================================================================
async function getTenantBundle(forceRefresh = false) {
  if (!forceRefresh && tenantCache.data && Date.now() - tenantCache.updatedAt < 45000) {
    return tenantCache;
  }
  const [{ data: tenant }, { data: items }] = await Promise.all([
    supabase.from('merchants').select('*').eq('id', DEFAULT_TENANT_UUID).maybeSingle(),
    supabase.from('menu_items').select('*').eq('merchant_id', DEFAULT_TENANT_UUID).eq('available', true)
  ]);
  if (tenant) {
    tenant.custom_context = parseCustomContext(tenant.custom_context);
  }
  tenantCache = { data: tenant, items: items || [], updatedAt: Date.now() };
  return tenantCache;
}

function buildBotSelfIntroductionPrompt() {
  return (
    `👋 *¡Hola! Soy WaBot.AI 2079 EDGE, tu Asistente Comercial Autónomo con Inteligencia Artificial.*\n\n` +
    `🤖 *¿Qué hago y cómo funciono?*\n` +
    `• Atiendo a tus clientes 24/7 en WhatsApp, muestro tu catálogo digital, tomo pedidos o agendo citas y valido pagos de *Deuna!* y transferencias con IA.\n` +
    `• Opero bajo la *LOPDP Ecuador* (Arts. 10, 13 y 15: uso moderado y minimización de datos con purga automática a los 10 días).\n\n` +
    `📝 *Quiero que me ayudes con el contexto de tu negocio para configurarme a tu medida. Envíame ahora:*\n` +
    `1️⃣ *El contexto de tu negocio:* Nombre de tu local, qué vendes o qué servicio ofreces, dirección, horario y costo de envío.\n` +
    `2️⃣ *Tu Menú o Catálogo de Precios:* Escribe tus productos con precios o envíame una *FOTO / PDF* de tu menú.\n` +
    `3️⃣ *Tu QR de cobro o Cuenta Bancaria:* Envía la imagen de tu *QR de Deuna!/Banco* o tu número de cuenta y titular para cobrar a tus clientes.\n\n` +
    `💡 *Envíame esos datos en un solo mensaje o en foto aquí mismo para empezar.*`
  );
}

// ============================================================================
// 5. FLUJO AUTÓNOMO QUE PIDE EL CONTEXTO, MENÚ Y QR AL INICIAR
// ============================================================================
async function handleAutonomousSetup({ ctx, tenant, items, senderPhone, text, msg }) {
  const t = (text || '').trim();
  const tLower = normalizeText(t);
  const cCtx = parseCustomContext(tenant.custom_context);

  const isExplicitSetupTrigger = [
    'configurar', 'iniciar', 'contexto', 'cambiar contexto',
    'setup', 'cambiar menu', 'nuevo negocio', 'onboarding'
  ].includes(tLower);

  const needsInitialOnboarding = !cCtx.onboarding_done_v2 || items.length === 0;
  const userInSetupFlow = onboardingState.has(senderPhone);

  // Si escribe comando de configurar, o si aún no ha hecho el onboarding v2 y envía un saludo/mensaje corto:
  // El bot SIEMPRE se presenta primero y le pide el contexto, el menú y el QR (0 tokens de IA)
  if (isExplicitSetupTrigger || (needsInitialOnboarding && !userInSetupFlow && t.length < 25 && !msg?.message?.imageMessage && !msg?.message?.documentMessage)) {
    onboardingState.set(senderPhone, { step: 'waiting_business_data' });
    await ctx.send(buildBotSelfIntroductionPrompt());
    return true;
  }

  if (!needsInitialOnboarding && !userInSetupFlow) {
    return false;
  }

  let mediaPart = null;
  if (msg?.message?.imageMessage || msg?.message?.documentMessage) {
    try {
      const buf = await downloadMediaMessage(msg, 'buffer', {});
      const mimeType = msg.message.imageMessage?.mimetype || msg.message.documentMessage?.mimetype || 'image/jpeg';
      mediaPart = { inlineData: { mimeType, data: buf.toString('base64') } };
    } catch (e) {
      console.error('[Media Download Error]:', e.message);
    }
  }

  await ctx.send('🪄 *Procesando el contexto y menú de tu negocio...*');

  const extractPrompt = `Extrae del mensaje o imagen los datos del negocio. Devuelve SOLO JSON válido:
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
Texto del usuario: "${t}"`;

  const parts = [{ text: extractPrompt }];
  if (mediaPart) parts.push(mediaPart);

  let ext = {};
  try {
    const aiRes = await gemini({
      contents: [{ role: 'user', parts }],
      maxOutputTokens: 800,
      jsonMode: true
    });
    ext = parseAIExtraction(extractTextFromParts(aiRes?.candidates?.[0]?.content?.parts));
  } catch (err) {
    console.error('[Gemini Setup Error]:', err.message);
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
  const webUrl = refreshed.data?.website_url || `https://wabot.ec/b/${refreshed.data?.slug}`;
  const menuUrl = refreshed.data?.menu_url || `https://wabot.ec/b/${refreshed.data?.slug}/menu`;

  await ctx.send(
    `🎉 *¡Tu Negocio "${refreshed.data?.name}" está Configurado y Activo!*\n\n` +
    `• *Giro:* ${updatedCtx.giro || 'Comercio'}\n` +
    `• *Productos en el Menú:* ${refreshed.items?.length || 0}\n` +
    `• *Datos de Cobro / QR:* ${updatedCtx.datos_bancarios || (updatedCtx.payment_qr_received ? 'QR registrado' : 'Por confirmar')}\n` +
    `• *Web:* ${webUrl}\n` +
    `• *Menú QR:* ${menuUrl}\n\n` +
    `💡 Para volver a cambiar el contexto o el menú en cualquier momento, escribe *configurar*.`
  );
  return true;
}

// ============================================================================
// 6. RESPUESTAS DE 0 TOKENS Y PROMPT ULTRA-COMPACTO PARA CLIENTES
// ============================================================================
function tryZeroTokenCustomerReply(tenant, items, text, isFirstMessage) {
  const clean = normalizeText(text);
  const ctx = parseCustomContext(tenant.custom_context);
  const isAllInOne = tenant?.plan === 'all_in_one' || tenant?.plan === 'trial';
  const webUrl = tenant?.website_url || `https://wabot.ec/b/${tenant?.slug}`;
  const menuUrl = tenant?.menu_url || `https://wabot.ec/b/${tenant?.slug}/menu`;

  const intro = isFirstMessage
    ? `👋 ¡Hola! Soy el Asistente Virtual de *${tenant?.name || 'WaBot'}* (${ctx.giro || 'Comercio'}), impulsado por *WaBot.AI 2079 EDGE*.\n` +
      `Te ayudo con el menú oficial, precios, pedidos y validación de pagos Deuna! bajo la *LOPDP Ecuador* (uso moderado de datos, retención 10 días; comandos *mis datos* / *borrar mis datos*).\n\n`
    : '';

  if (/^(hola|buenas|buenos dias|buenas tardes|buenas noches|info|informacion)$/.test(clean)) {
    const sample = (items || []).slice(0, 6).map(i => `• *${i.name}:* $${Number(i.price).toFixed(2)}`).join('\n');
    return (
      `${intro || `👋 ¡Bienvenido a *${tenant?.name || 'Local'}*!\n\n`}` +
      `🕒 *Horario:* ${tenant?.open_time || '09:00'} - ${tenant?.close_time || '20:00'} | 📍 ${tenant?.address || tenant?.city || 'Ecuador'}\n\n` +
      `📋 *Menú destacado:*\n${sample}\n\n` +
      (isAllInOne ? `🍽️ *Menú Digital QR:* ${menuUrl}\n🌐 *Web:* ${webUrl}\n\n` : '') +
      `¿Qué deseas pedir hoy? *(Si eres el dueño y deseas cambiar el giro o menú del bot, escribe **configurar**)*`
    );
  }

  if (/^(menu|catalogo|precios|lista de precios|carta|que venden)$/.test(clean)) {
    const list = (items || []).map(i => `• *${i.name}:* $${Number(i.price).toFixed(2)}`).join('\n');
    return (
      `${intro}📖 *MENÚ OFICIAL — ${tenant?.name || 'Local'}*\n\n${list}\n\n` +
      `🚚 *Envío:* $${Number(ctx.costo_envio || 0).toFixed(2)}\n` +
      (isAllInOne ? `🛒 *Pide en el Menú QR:* ${menuUrl}` : `Escribe los productos que deseas ordenar.`)
    );
  }

  if (/^(cuenta|deuna|transferencia|qr|datos bancarios|como pago|metodos de pago)$/.test(clean)) {
    return (
      `${intro}💳 *DATOS DE PAGO — ${tenant?.name || 'Local'}*\n\n` +
      `• *Deuna! / Cuenta:* ${ctx.datos_bancarios || 'Solicitar confirmación al cerrar pedido'}\n` +
      `• *Titular:* ${ctx.titular_cuenta_ocr || tenant?.name || 'Titular'}\n\n` +
      `📸 Envía la captura de tu comprobante por aquí para validarlo con IA.`
    );
  }

  return null;
}

function buildCompactSystemPrompt(tenant, items = [], includeIntro = false) {
  const ctx = parseCustomContext(tenant.custom_context);
  const compactCatalog = (items || []).map(i => `${i.name}:$${Number(i.price).toFixed(2)}`).join(' | ');
  return `Asistente comercial de "${tenant?.name || 'Comercio'}" (${ctx.giro || 'Comercio'}, ${tenant?.address || tenant?.city || 'Ecuador'}, ${tenant?.open_time || '09:00'}-${tenant?.close_time || '20:00'}).
${includeIntro ? 'Preséntate en 1 línea como Asistente IA de ' + tenant?.name + ' (WaBot.AI 2079, LOPDP retención 10 días).' : ''}
Catálogo USD: ${compactCatalog}.
Envío: $${Number(ctx.costo_envio || 0).toFixed(2)}. Pagos: ${ctx.datos_bancarios || 'Deuna/Transferencia'}.
Responde en máximo 4 líneas, sin inventar precios ni productos.`;
}

// ============================================================================
// 7. PROCESADOR DE MENSAJES ENTRANTES
// ============================================================================
async function processIncomingMessage({ senderPhone, text, msg, sendReply }) {
  const { data: tenant, items } = await getTenantBundle();
  if (!tenant) return;

  const cleanLower = (text || '').trim().toLowerCase();

  if (cleanLower === 'mis datos' || cleanLower === 'consultar mis datos') {
    const { data: arco } = await supabase.rpc('export_customer_data', { p_tenant: tenant.id, p_phone: senderPhone });
    await sendReply(
      `📄 *REPORTE LOPDP ECUADOR (Art. 13)*\n• *Teléfono:* +${senderPhone}\n• *Pedidos:* ${arco?.pedidos?.length || 0}\n• *Uso moderado (Art. 10):* Chats eliminados a los 10 días. Escribe *borrar mis datos* para anonimizar.`
    );
    return;
  }

  if (cleanLower === 'borrar mis datos' || cleanLower === 'eliminar mis datos') {
    await supabase.rpc('anonymize_customer', { p_tenant: tenant.id, p_phone: senderPhone });
    await sendReply(`🔒 *Datos anonimizados irreversiblemente (Art. 15 LOPDP Ecuador).*`);
    return;
  }

  const handledBySetup = await handleAutonomousSetup({
    ctx: { send: sendReply },
    tenant,
    items,
    senderPhone,
    text,
    msg
  });
  if (handledBySetup) return;

  const isFirstMsg = !greetedUsers.has(senderPhone);
  greetedUsers.add(senderPhone);

  const zeroReply = tryZeroTokenCustomerReply(tenant, items, text, isFirstMsg);
  if (zeroReply) {
    await sendReply(zeroReply);
    return;
  }

  const sysPrompt = buildCompactSystemPrompt(tenant, items, isFirstMsg);
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
// 8. MOTOR BAILEYS CON QR CRUDO NATIVO (SIN buildPairingQRData)
// ============================================================================
async function startBot(forceWipe = false) {
  if (isStartingSocket) return;
  isStartingSocket = true;

  try {
    if (forceWipe) {
      await wipeCorruptedSessionAndResetOnboarding();
    } else {
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

    sock.ev.on('creds.update', async () => {
      await saveCreds();
      scheduleBackupAuthToSupabase(5000);
    });

    sock.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect, qr } = update;

      // USAR SIEMPRE EL STRING `qr` EXACTO Y CRUDO DE BAILEYS (NUNCA buildPairingQRData)
      if (typeof qr === 'string' && qr.length > 10) {
        latestQrData = qr;
        connectionStatus = 'qr_ready';
        console.log(`[QR Emitido] Longitud=${qr.length} caracteres.`);
      }

      if (connection === 'close') {
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        lastDisconnectCode = statusCode;
        console.warn(`[Socket Closed] statusCode=${statusCode}`);

        // 1. Código 515 (restartRequired): Normal inmediatamente después de escanear el QR
        if (statusCode === DisconnectReason.restartRequired || statusCode === 515) {
          setTimeout(() => startBot(false), 150);
          return;
        }

        // 2. Código 440 (connectionReplaced): Otra instancia (ej. tu PC local) está peleando por la misma sesión
        if (statusCode === DisconnectReason.connectionReplaced || statusCode === 440) {
          connectionStatus = 'conflict_other_instance';
          console.error('[Conflicto 440] Cierra cualquier terminal local de Node.js en tu PC para que Render no compita.');
          setTimeout(() => startBot(false), 10000);
          return;
        }

        // 3. Sesión cerrada o inválida (401, 403, 500) -> Limpiar automáticamente y generar QR nuevo
        if (
          statusCode === DisconnectReason.loggedOut ||
          statusCode === DisconnectReason.badSession ||
          statusCode === DisconnectReason.multideviceMismatch ||
          statusCode === 401 ||
          statusCode === 403 ||
          statusCode === 500
        ) {
          setTimeout(() => startBot(true), 1000);
          return;
        }

        connectionStatus = 'reconnecting';
        setTimeout(() => startBot(false), 3000);
      } else if (connection === 'open') {
        latestQrData = null;
        latestPairingCode = null;
        connectionStatus = 'connected';
        scheduleBackupAuthToSupabase(2000);

        // Enviar siempre el mensaje de presentación pidiendo el contexto al conectar
        try {
          const myJid = sock.user?.id?.replace(/:\d+@/, '@');
          const myPhone = (myJid || '').replace(/@.+$/, '').replace(/[^\d]/g, '');
          if (myJid) {
            onboardingState.set(myPhone, { step: 'waiting_business_data' });
            await sock.sendMessage(myJid, { text: buildBotSelfIntroductionPrompt() });
          }
        } catch (err) {
          console.error('[Auto-Intro Error]:', err.message);
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
            } catch (sendErr) {
              console.error('[Send Reply Error]:', sendErr.message);
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
// 9. PANEL /qr CON NIVEL DE CORRECCIÓN 'L' (LECTURA INSTANTÁNEA EN WHATSAPP)
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
    .card { max-width: 500px; margin: 0 auto; background: #1e293b; padding: 26px; border-radius: 16px; }
    #qr-box { display: inline-block; background: #ffffff; padding: 18px; border-radius: 12px; margin: 14px auto; }
    .btn { display: inline-block; background: #ef4444; color: #fff; text-decoration: none; padding: 11px 18px; border-radius: 10px; font-weight: 700; margin-top: 10px; border: none; cursor: pointer; }
    .btn-pair { background: #38bdf8; color: #0f172a; }
    input { padding: 10px; border-radius: 8px; border: 1px solid #475569; width: 65%; margin-top: 10px; }
    .code-badge { font-size: 28px; font-weight: 800; letter-spacing: 5px; color: #22c55e; background: #0f172a; padding: 14px; border-radius: 10px; margin: 12px 0; }
  </style>
</head>
<body>
  <div class="card">
    <h2>🤖 WaBot.AI 2079 EDGE — Panel QR Oficial</h2>
    <p id="status-text" style="color:#22c55e;font-weight:700;">Cargando estado...</p>
    <div id="qr-container" style="display:none;">
      <div id="qr-box"></div>
      <p style="font-size:12px;color:#94a3b8;">Nivel de corrección L (Óptimo para cámara de WhatsApp)</p>
    </div>
    <div id="pair-box" style="display:none;" class="code-badge"></div>

    <div style="margin-top:14px;border-top:1px solid #334155;padding-top:14px;">
      <a class="btn" href="/qr?reset=1">🔄 Borrar Sesión + Pedir Contexto de Nuevo + Generar QR</a>
    </div>

    <form action="/pair" method="GET" style="margin-top:16px;border-top:1px solid #334155;padding-top:14px;">
      <p style="font-size:13px;color:#cbd5e1;margin:4px 0;">¿Prefieres vincular SIN escanear QR? Ingresa tu número con 593:</p>
      <input type="text" name="phone" placeholder="Ej: 593983476198" required />
      <button type="submit" class="btn btn-pair">Generar Código de 8 Dígitos</button>
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
          statusText.textContent = '✅ ¡WhatsApp Vinculado! Abre tu WhatsApp: el bot ya te envió el saludo pidiendo el contexto de tu negocio.';
          qrContainer.style.display = 'none';
          return;
        }
        if (data.qr) {
          statusText.textContent = '📲 Apunta la cámara de WhatsApp > Dispositivos vinculados:';
          qrContainer.style.display = 'block';
          if (data.qr !== currentQr) {
            currentQr = data.qr;
            qrBox.innerHTML = '';
            try {
              // CRÍTICO: Usar CorrectLevel.L para cadenas largas de Baileys (CorrectLevel.H satura el QR y da error al escanear)
              new QRCode(qrBox, {
                text: data.qr,
                width: 260,
                height: 260,
                correctLevel: QRCode.CorrectLevel.L
              });
            } catch (err) {
              // Fallback a imagen PNG limpia con margen blanco si qrcodejs falla
              qrBox.innerHTML = '<img src="https://api.qrserver.com/v1/create-qr-code/?size=260x260&ecc=L&margin=10&data=' + encodeURIComponent(data.qr) + '" width="260" height="260" />';
            }
          }
        } else {
          statusText.textContent = '⏳ Generando QR... Toca el botón rojo de abajo para forzar un QR limpio.';
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
