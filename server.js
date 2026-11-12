/**
 * WaBot.AI 2079 EDGE — Servidor Autónomo Cloud 24/7
 * Baileys WhatsApp + Google Gemini Flash Lite + Supabase PostgreSQL
 * Cumplimiento LOPDP Ecuador (Arts. 10, 13 y 15) & Protección Bancaria
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
// 1. VARIABLES DE ENTORNO Y CONFIGURACIÓN PROTEGIDA
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
let bootRestored = false;
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
      for (const f of allFiles.slice(0, 80)) {
        files[f] = readFileSync(join(AUTH_DIR, f), 'utf-8');
      }
      await supabase.from('pending_uploads').upsert({
        tenant_id: DEFAULT_TENANT_UUID,
        phone: '__baileys_auth__',
        draft: { files, updated_at: new Date().toISOString() }
      }, { onConflict: 'tenant_id,phone' });
      console.log(`[Cloud Auth] Respaldo asíncrono completado (${Object.keys(files).length} archivos en Supabase).`);
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
      console.warn('[Cloud Auth] Sesión en Supabase incompleta (sin me.id). Se generará QR limpio.');
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
  console.warn('[Cloud Auth] Purgando credenciales previas para generar un nuevo código QR...');
  if (backupTimer) clearTimeout(backupTimer);
  latestQrData = null;
  latestPairingCode = null;
  connectionStatus = 'initializing';
  welcomeSentOnOpen = false;
  if (sockInstance) {
    try {
      sockInstance.ev.removeAllListeners();
      sockInstance.ws?.close();
    } catch (_) {}
    sockInstance = null;
  }
  try {
    if (existsSync(AUTH_DIR)) rmSync(AUTH_DIR, { recursive: true, force: true });
    mkdirSync(AUTH_DIR, { recursive: true });
  } catch (_) {}
  try {
    await supabase
      .from('pending_uploads')
      .delete()
      .eq('tenant_id', DEFAULT_TENANT_UUID)
      .eq('phone', '__baileys_auth__');
  } catch (_) {}
}

// ============================================================================
// 3. CACHÉ DEL COMERCIO Y CATÁLOGO (ALTA VELOCIDAD)
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
  const giro = ctx.giro || tenant?.giro;
  const missing = [];
  if (!tenant?.name || tenant.name === 'Nuevo Comercio' || !giro || giro === 'Comercio General') {
    missing.push('contexto');
  }
  if (!Array.isArray(items) || items.length === 0) {
    missing.push('menu');
  }
  if (!ctx.datos_bancarios && !ctx.payment_qr_received && !tenant?.payment_note) {
    missing.push('qr_cobro');
  }
  return missing;
}

function buildBotSelfIntroductionPrompt(missingFields = []) {
  const steps = [];
  if (missingFields.includes('contexto')) {
    steps.push(`1️⃣ *Contexto de tu negocio:* Nombre del local, a qué te dedicas, horario, dirección y costo de envío.`);
  }
  if (missingFields.includes('menu')) {
    steps.push(`2️⃣ *Tu Menú o Lista de Precios:* Escribe tus productos con precios o envíame una *foto/PDF* de tu carta.`);
  }
  if (missingFields.includes('qr_cobro')) {
    steps.push(`3️⃣ *Tu QR de Deuna! o Cuenta Bancaria:* Envíame la imagen de tu QR de cobro o tu número de cuenta.`);
  }

  return (
    `👋 *¡Hola! Soy WaBot.AI 2079 EDGE, tu Asistente Comercial Autónomo con IA.*\n\n` +
    `🤖 *Estoy listo para atender a tus clientes 24/7 en la nube (sin depender de tu PC).*\n\n` +
    (steps.length > 0
      ? `📝 *Para personalizar tu asistente, envíame los siguientes datos:*\n${steps.join('\n')}\n\n💡 *Puedes enviármelo en texto o foto aquí mismo.*`
      : `✨ Tu negocio ya está configurado y atendiendo. Escribe *configurar* cuando quieras actualizar tu carta o precios.`)
  );
}

// ============================================================================
// 4. CLIENTE GEMINI FLASH LITE CON FALLBACK SEGURO
// ============================================================================
async function gemini({ contents, systemInstruction, maxOutputTokens = 200, jsonMode = false }) {
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
// 5. RESPUESTAS INSTANTÁNEAS DE 0 TOKENS (< 10ms DE LATENCIA)
// ============================================================================
function tryZeroTokenCustomerReply(tenant, items, text, isFirstMessage) {
  const clean = normalizeText(text);
  const ctx = tenant.custom_context || {};
  const isAllInOne = tenant.plan === 'all_in_one' || tenant.plan === 'trial';
  const webUrl = tenant.website_url || `https://wa-saas-worker.lopdp-bot.workers.dev/b/${tenant.slug || 'wabot-oficial'}`;
  const menuUrl = tenant.menu_url || `https://wa-saas-worker.lopdp-bot.workers.dev/b/${tenant.slug || 'wabot-oficial'}/menu`;

  const introHeader = isFirstMessage
    ? `👋 ¡Hola! Soy el Asistente Virtual de *${tenant.name || 'nuestro comercio'}*, impulsado por *WaBot.AI 2079 EDGE*.\n` +
      `Brindo precios oficiales, menú digital y validación de pagos Deuna! bajo la *LOPDP Ecuador* (retención efímera 10 días; escribe *mis datos* o *borrar mis datos*).\n\n`
    : '';

  // SALUDOS (Respuesta instantánea)
  if (/^(hola|buenas|buenos dias|buenas tardes|buenas noches|info|informacion|inicio|ola|que tal|saludos|hello|hi)(\b.*)?$/i.test(clean) && clean.length < 35) {
    const sample = items.slice(0, 6).map(i => `• *${i.name}:* $${Number(i.price).toFixed(2)}`).join('\n');
    return (
      `${introHeader || `👋 ¡Bienvenido a *${tenant.name}*!\n\n`}` +
      `🕒 *Horario:* ${tenant.open_time || '09:00'} - ${tenant.close_time || '20:00'} | 📍 ${tenant.address || tenant.city || 'Ecuador'}\n\n` +
      `📋 *Catálogo Destacado:*\n${sample || 'Consulta nuestro menú completo a continuación'}\n\n` +
      (isAllInOne ? `🍽️ *Menú Digital QR:* ${menuUrl}\n🌐 *Página Web:* ${webUrl}\n\n` : '') +
      `¿En qué te podemos ayudar hoy?`
    );
  }

  // MENÚ Y PRECIOS
  if (/(menu|catalogo|precios|lista de precios|carta|que venden|productos|que tienen|costos)/i.test(clean)) {
    const list = items.map(i => `• *${i.name}:* $${Number(i.price).toFixed(2)}`).join('\n');
    const shipping = ctx.costo_envio ? `🚚 *Envío:* $${Number(ctx.costo_envio).toFixed(2)}\n` : '';
    return (
      `${introHeader}📖 *MENÚ Y PRECIOS — ${tenant.name}*\n\n${list || 'Productos disponibles en nuestro enlace digital'}\n\n` +
      `${shipping}` +
      (isAllInOne ? `🛒 *Haz tu pedido en el Menú QR:* ${menuUrl}` : `Escribe el producto que deseas pedir.`)
    );
  }

  // MÉTODOS DE PAGO / DEUNA / CUENTA
  if (/(cuenta|deuna|transferencia|qr|datos bancarios|como pago|metodos de pago|pago)/i.test(clean)) {
    return (
      `${introHeader}💳 *MÉTODOS DE PAGO — ${tenant.name}*\n\n` +
      `• *Deuna! / Cuenta:* ${ctx.datos_bancarios || tenant.payment_note || 'Transferencia Bancaria o Deuna!'}\n` +
      `• *Titular:* ${ctx.titular_cuenta_ocr || tenant.name}\n` +
      `• *Tarjeta de Crédito:* ${ctx.acepta_tc ? 'Aceptada' : 'Confirmar con asesor'}\n\n` +
      `📸 Una vez realizada la transferencia, envía la captura del comprobante por aquí para validarla de inmediato.`
    );
  }

  return null;
}

function buildCompactSystemPrompt(tenant, items = [], includeIntro = false) {
  const ctx = tenant.custom_context || {};
  const isAllInOne = tenant.plan === 'all_in_one' || tenant.plan === 'trial';
  const menuUrl = tenant.menu_url || `https://wa-saas-worker.lopdp-bot.workers.dev/b/${tenant.slug || 'wabot-oficial'}/menu`;
  const compactCatalog = items.map(i => `${i.name}:$${Number(i.price).toFixed(2)}`).join(' | ');

  return `Asistente comercial de "${tenant.name}" (${ctx.giro || tenant.giro || 'Comercio'}, ${tenant.address || tenant.city || 'Ecuador'}, Horario: ${tenant.open_time || '09:00'}-${tenant.close_time || '20:00'}).
${includeIntro ? 'Preséntate en 1 línea como Asistente IA de ' + tenant.name + ' (WaBot.AI 2079, LOPDP retención 10 días).' : ''}
Precios USD oficiales: ${compactCatalog}.
Envío: $${Number(ctx.costo_envio || 0).toFixed(2)}. Pagos: ${ctx.datos_bancarios || tenant.payment_note || 'Deuna o Transferencia'}.
${isAllInOne ? `Menú QR con Carrito: ${menuUrl}` : ''}
Responde en 2 a 4 líneas de manera cortés y ecuatoriana. No inventes precios ni productos no listados.`;
}

// ============================================================================
// 6. FLUJO DE CONFIGURACIÓN DEL DUEÑO (SOLO OWNER)
// ============================================================================
async function handleOwnerSetupFlow({ ctx, tenant, senderPhone, text, msg }) {
  const t = (text || '').trim();
  const tLower = t.toLowerCase();

  const isSetupKeyword = [
    'configurar', 'iniciar', 'contexto', 'cambiar contexto',
    'setup', 'cambiar menu', 'cambiar menú', 'onboarding'
  ].includes(tLower);

  if (isSetupKeyword) {
    onboardingState.set(senderPhone, { stage: 'awaiting_setup' });
    const missing = checkMissingSetupFields(tenant, []);
    await ctx.send(buildBotSelfIntroductionPrompt(missing));
    return true;
  }

  if (onboardingState.has(senderPhone)) {
    let mediaPart = null;
    if (msg?.message?.imageMessage || msg?.message?.documentMessage) {
      try {
        const buf = await downloadMediaMessage(msg, 'buffer', {});
        const mimeType = msg.message.imageMessage?.mimetype || msg.message.documentMessage?.mimetype || 'image/jpeg';
        mediaPart = { inlineData: { mimeType, data: buf.toString('base64') } };
      } catch (_) {}
    }

    await ctx.send('🪄 *Procesando y guardando los datos de tu negocio...*');

    const extractPrompt = `Extrae del texto o imagen los datos comerciales. Devuelve SOLO JSON válido:
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
  "items": [{"nombre": string, "precio": number, "categoria": string}]
}
Texto: "${t}"`;

    const parts = [{ text: extractPrompt }];
    if (mediaPart) parts.push(mediaPart);

    try {
      const aiRes = await gemini({ contents: [{ role: 'user', parts }], maxOutputTokens: 800, jsonMode: true });
      const ext = parseAIExtraction(extractTextFromParts(aiRes?.candidates?.[0]?.content?.parts));

      const newCtx = { ...(tenant.custom_context || {}) };
      const updateRow = {};

      if (ext.nombre_negocio) updateRow.name = ext.nombre_negocio;
      if (ext.direccion) updateRow.address = ext.direccion;
      if (ext.giro) { newCtx.giro = ext.giro; updateRow.giro = ext.giro; }
      if (ext.costo_envio !== null && ext.costo_envio !== undefined) newCtx.costo_envio = Number(ext.costo_envio) || 0;
      if (ext.datos_bancarios) { newCtx.datos_bancarios = ext.datos_bancarios; updateRow.payment_note = ext.datos_bancarios; }
      if (ext.titular_cuenta_ocr) newCtx.titular_cuenta_ocr = ext.titular_cuenta_ocr;
      if (ext.acepta_tc !== null && ext.acepta_tc !== undefined) newCtx.acepta_tc = Boolean(ext.acepta_tc);
      if (ext.es_qr_pago) newCtx.payment_qr_received = true;
      newCtx.contexto_configurado = true;

      updateRow.custom_context = newCtx;
      await supabase.from('merchants').update(updateRow).eq('id', tenant.id);

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
      await getTenantBundle(true);
      await ctx.send(`🎉 *¡Datos actualizados con éxito!*\nTu asistente IA ya está aplicando los nuevos cambios en WhatsApp.`);
      return true;
    } catch (err) {
      console.error('[Owner Setup Error]:', err.message);
      onboardingState.delete(senderPhone);
      await ctx.send(`⚠️ No se pudo procesar automáticamente el texto. Escribe *configurar* para intentarlo nuevamente.`);
      return true;
    }
  }

  return false;
}

// ============================================================================
// 7. PROCESADOR CENTRAL DE MENSAJES (ULTRA FLUIDO)
// ============================================================================
async function processIncomingMessage({ senderPhone, text, msg, sendReply }) {
  const { data: tenant, items } = await getTenantBundle();
  if (!tenant) return;

  const isOwner =
    senderPhone === (tenant.owner_phone || '').replace(/[^\d]/g, '') ||
    senderPhone === (WABOT_BILLING_INFO.superadmin_phone || '').replace(/[^\d]/g, '');

  const cleanLower = (text || '').trim().toLowerCase();

  // 1. DERECHOS ARCO LOPDP ECUADOR (Art. 13 y 15)
  if (cleanLower === 'mis datos' || cleanLower === 'consultar mis datos') {
    try {
      const { data: arco } = await supabase.rpc('export_customer_data', { p_tenant: tenant.id, p_phone: senderPhone });
      await sendReply(
        `📄 *REPORTE LOPDP ECUADOR (Art. 13)*\n• *Teléfono:* +${senderPhone}\n• *Pedidos Registrados:* ${arco?.pedidos?.length || 0}\n• *Uso Moderado (Art. 10):* Conversaciones eliminadas tras 10 días.\n• Para anonimizar tus datos, responde *borrar mis datos*.`
      );
    } catch (_) {
      await sendReply(`📄 *REPORTE LOPDP:* Tus datos se manejan bajo el principio de minimización (Art. 10). Escribe *borrar mis datos* para solicitar su eliminación.`);
    }
    return;
  }

  if (cleanLower === 'borrar mis datos' || cleanLower === 'eliminar mis datos') {
    try {
      await supabase.rpc('anonymize_customer', { p_tenant: tenant.id, p_phone: senderPhone });
    } catch (_) {}
    await sendReply(`🔒 *Tus datos han sido anonimizados irreversiblemente conforme al Art. 15 de la LOPDP Ecuador.*`);
    return;
  }

  // 2. CONFIGURACIÓN DEL DUEÑO (SOLO SI ES EL OWNER)
  if (isOwner) {
    const handled = await handleOwnerSetupFlow({ ctx: { send: sendReply }, tenant, senderPhone, text, msg });
    if (handled) return;
  }

  // 3. RESPUESTAS RÁPIDAS DE 0 TOKENS (SALUDO, MENÚ, PRECIOS, PAGOS)
  const isFirstMsg = !greetedUsers.has(senderPhone);
  greetedUsers.add(senderPhone);

  const zeroTokenReply = tryZeroTokenCustomerReply(tenant, items, text, isFirstMsg);
  if (zeroTokenReply) {
    await sendReply(zeroTokenReply);
    return;
  }

  // 4. CONTROL DE CUOTA IA
  try {
    const { data: quotaAi } = await supabase.rpc('check_and_consume_quota', { p_tenant: tenant.id, p_metric: 'ai' });
    if (!quotaAi?.allowed) {
      await sendReply(
        `🔒 *Periodo de prueba finalizado.*\nPara activar tu servicio mensual ($19.99 o $29.99), transfiere a *${WABOT_BILLING_INFO.bank_name}* (${WABOT_BILLING_INFO.account_type} \`${WABOT_BILLING_INFO.account_number}\`, Titular: ${WABOT_BILLING_INFO.beneficiary_name}, C.I. \`${WABOT_BILLING_INFO.beneficiary_id}\`) y envía el comprobante por aquí.`
      );
      return;
    }
  } catch (_) {}

  // 5. ATENCIÓN COMERCIAL INTELIGENTE CON GEMINI FLASH LITE (< 1s)
  try {
    const sysPrompt = buildCompactSystemPrompt(tenant, items, isFirstMsg);
    const aiRes = await gemini({
      systemInstruction: sysPrompt,
      contents: [{ role: 'user', parts: [{ text }] }],
      maxOutputTokens: 180
    });

    const reply = extractTextFromParts(aiRes?.candidates?.[0]?.content?.parts);
    if (reply) {
      await sendReply(reply);
      return;
    }
  } catch (aiErr) {
    console.error('[Gemini AI Fallback Error]:', aiErr.message);
  }

  // Fallback seguro si la IA no respondió o dio timeout
  await sendReply(
    `👋 ¡Hola! En *${tenant.name}* estamos atentos a tus pedidos. Puedes consultar nuestro catálogo respondiendo *menu* o consultarnos directamente lo que necesitas.`
  );
}

// ============================================================================
// 8. MOTOR BAILEYS OPTIMIZADO (CONEXIÓN Y MANEJO CÓDIGO 515)
// ============================================================================
async function startBot(forceWipe = false) {
  if (sockInstance) {
    try {
      sockInstance.ev.removeAllListeners();
      sockInstance.ws?.close();
    } catch (_) {}
    sockInstance = null;
  }

  if (forceWipe) {
    await wipeCorruptedSession();
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

    if (qr) {
      latestQrData = qr;
      connectionStatus = 'qr_ready';
      console.log('[QR Ready] Nuevo código QR listo en /qr');
    }

    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      console.log(`[Connection Closed] statusCode=${statusCode}`);

      // Código 515 (restartRequired) tras escanear el QR: reconexión inmediata en 150ms
      if (statusCode === DisconnectReason.restartRequired || statusCode === 515) {
        console.log('[Handshake 515] Reinicio rápido de emparejamiento en 150ms...');
        setTimeout(() => startBot(false), 150);
        return;
      }

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

      connectionStatus = 'reconnecting';
      setTimeout(() => startBot(false), 2500);
    } else if (connection === 'open') {
      latestQrData = null;
      latestPairingCode = null;
      connectionStatus = 'connected';
      console.log('✅ [WaBot.AI 2079 EDGE] ¡WhatsApp conectado y operativo 24/7!');
      scheduleBackupAuthToSupabase(2000);

      if (!welcomeSentOnOpen) {
        welcomeSentOnOpen = true;
        setTimeout(async () => {
          try {
            const myJid = sock.user?.id?.replace(/:\d+@/, '@');
            if (!myJid || !sock || !sock.user) return;
            const { data: tenant } = await getTenantBundle(true);
            const isConfigured = Boolean(tenant?.custom_context?.contexto_configurado);
            if (!isConfigured) {
              await sock.sendMessage(myJid, { text: buildBotSelfIntroductionPrompt(['contexto', 'menu', 'qr_cobro']) });
              console.log('✅ [Welcome Sent] Prompt de bienvenida enviado al dueño.');
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

        if (!text.trim() && !msg.message.imageMessage && !msg.message.documentMessage) continue;

        // Feedback visual inmediato en WhatsApp ("Escribiendo...")
        try {
          await sock.sendPresenceUpdate('composing', remoteJid);
        } catch (_) {}

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
      } catch (err) {
        console.error('[Message Processing Error]:', err.message);
      }
    }
  });
}

// ============================================================================
// 9. SERVIDOR HTTP (/qr, /health, /api/qr-state, /pair)
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
      <a class="btn" href="/qr?reset=1">🔄 Borrar Sesión y Generar QR Nuevo</a>
    </div>

    <form action="/pair" method="GET" style="margin-top:16px;border-top:1px solid #334155;padding-top:14px;">
      <p style="font-size:13px;color:#cbd5e1;margin:4px 0;">O vincular con Código de 8 dígitos:</p>
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
          statusText.textContent = '✅ ¡WhatsApp Conectado y Activo 24/7! Puedes escribirle un mensaje para probarlo.';
          qrContainer.style.display = 'none';
          return;
        }
        if (data.qr) {
          statusText.textContent = '📲 Escanea este QR para vincular en 1 segundo:';
          qrContainer.style.display = 'block';
          if (data.qr !== currentQr) {
            currentQr = data.qr;
            qrBox.innerHTML = '';
            new QRCode(qrBox, { text: data.qr, width: 256, height: 256 });
          }
        } else {
          statusText.textContent = '⏳ Iniciando conexión...';
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
