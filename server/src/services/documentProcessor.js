// server/src/services/documentProcessor.js
import { DocumentProcessorServiceClient } from "@google-cloud/documentai";
import Parser from "name-parser";
import { config } from "../config/index.js";
import logger from '../utils/logger.js';
import fs from "fs";
import path from "path";
import pLimit from "p-limit";
import { Worker } from "worker_threads";
import os from "os";
import { promises as fsPromises } from "fs";
// import pkg from 'name-parser';
// const { Parser } = pkg;



// --- CONFIGURATION & CONSTANTS ---
const SAFE_MAX_WORKERS = 24; // Upper bound for high-resource environment (8 vGPU/64GB) - allows aggressive parallelization
const BASE_WORKER_THREAD_POOL = Number(config.workerThreadPoolSize) || Math.max(2, Math.min(os.cpus().length, 16)); // configurable base pool size
const MAX_PDF_SIZE_BYTES = 50 * 1024 * 1024; // 50MB warning threshold
const PDF_SIZE_WARN_BYTES = 30 * 1024 * 1024; // 30MB soft limit
// Increased for high-resource environment (8 vGPU/64GB) to reduce DB round-trips during bulk inserts
const BATCH_SIZE_RECORDS = 5000; // Increased for high-resource environment (8 vGPU/64GB)
const RETRY_ATTEMPTS = parseInt(process.env.RETRY_ATTEMPTS, 10) || 3;
const INITIAL_BACKOFF_MS = parseInt(process.env.INITIAL_BACKOFF_MS, 10) || 1000;

// Document AI request timeout (ms). Default to 20 minutes (1200000ms). Allow override via env.
const _requestedTimeout = parseInt(process.env.REQUEST_TIMEOUT_MS, 10);
const REQUEST_TIMEOUT_MS = (Number.isFinite(_requestedTimeout) && _requestedTimeout >= 60000) ? _requestedTimeout : 1200000;
if (!Number.isFinite(_requestedTimeout) && process.env.REQUEST_TIMEOUT_MS) {
  logger.warn('REQUEST_TIMEOUT_MS invalid; using default 1200000');
} else if (Number.isFinite(_requestedTimeout) && _requestedTimeout < 60000) {
  logger.warn('REQUEST_TIMEOUT_MS too small; minimum is 60000ms. Using default 1200000');
}



// --- Global State Management ---
let client;
let workerThreadPool = null;
let activeRequests = 0;
let currentScaledWorkers = null; // dynamic reference set at runtime inside processPDFs
// NOTE: Raised to match SAFE_MAX_WORKERS for high-performance environments so large batches can reach
// the requested scaled worker counts (e.g. 120 workers for 100-file batches).
const MAX_CONCURRENT_REQUESTS = 150; // Hard cap for in-flight Document AI requests (ceiling)


try {
  const clientConfig = {};
  if (process.env.NODE_ENV !== 'production') {
    clientConfig.keyFilename = config.credentials;
  }
  client = new DocumentProcessorServiceClient(clientConfig);
  logger.info('Document AI client initialized successfully');
} catch (error) {
  logger.error("Failed to initialize Document AI client:", error);
  // Do not throw here, as it can crash the server on startup.
  // The error will be handled in processPDFs.
}



// --- OPTIMIZATION 6: Pre-compiled Regex Patterns ---
const REGEX_PATTERNS = {
  addressStatePostcodeStart: /^\s*([A-Za-z]{2,3})\s+(\d{4})\s+(.+)$/i,
  addressPostcodeStateEnd: /^\s*(\d{4})\s+(.+?)\s+([A-Za-z]{2,3})\s*$/i,
  addressStatePostcodeMiddle: /^(.+?)\s+([A-Za-z]{2,3})\s+(\d{4})\s+(.+)$/i,
  addressStatePostcodeAny: /([A-Za-z]{2,3})\s+(\d{4})/i,
  nameInvalidChars: /[^A-Za-zÀ-ÖØ-öø-ÿ'\-\s]/g,
  nameSpecialChars: /�|･･･|…|•|\u2026/g,
  dateInvalidChars: /[^0-9A-Za-z\s\-\/]/g,
  dateFormat: /^(\d{1,2})([A-Za-z]{3,})(\d{4})$/,
  dashNormalize: /[-\u2013\u2014]+/g,
  dashMultiple: /-{2,}/g,
  dashTrim: /^[\-\s]+|[\-\s]+$/g,
  whitespaceMultiple: /\s+/g,
  digitOnly: /\D/g,
};



// --- WORKER THREAD POOL MANAGEMENT ---
class WorkerThreadPool {
  constructor(poolSize) {
    this.poolSize = poolSize;
    this.workers = [];
    this.taskQueue = [];
    this.activeCount = 0;
    this.initialize();
  }


  initialize() {
    for (let i = 0; i < this.poolSize; i++) {
      this.workers.push({
        isAvailable: true,
        worker: null, // Lazily initialized
      });
    }
    logger.info(`Worker thread pool initialized with ${this.poolSize} slots`);
  }


  async runTask(task) {
    return new Promise((resolve, reject) => {
      const availableWorker = this.workers.find(w => w.isAvailable);


      if (availableWorker) {
        this.executeOnWorker(availableWorker, task, resolve, reject);
      } else {
        this.taskQueue.push({ task, resolve, reject });
      }
    });
  }


  executeOnWorker(workerSlot, task, resolve, reject) {
    // PITFALL FIX: Lazy initialize workers to avoid startup overhead
    if (!workerSlot.worker) {
      workerSlot.worker = new Worker(new URL('./validators.worker.js', import.meta.url));
      workerSlot.worker.on('error', reject);
      workerSlot.worker.on('exit', (code) => {
        if (code !== 0) {
          reject(new Error(`Worker exited with code ${code}`));
        }
      });
    }


    workerSlot.isAvailable = false;
    this.activeCount++;


    const timeout = setTimeout(() => {
      reject(new Error('Worker task timeout'));
      workerSlot.isAvailable = true;
      this.activeCount--;
      this.processQueue();
    }, 60000); // 60s per worker task


    workerSlot.worker.once('message', (result) => {
      clearTimeout(timeout);
      workerSlot.isAvailable = true;
      this.activeCount--;


      if (result.error) {
        reject(new Error(result.error));
      } else {
        resolve(result);
      }


      this.processQueue();
    });


    workerSlot.worker.on('error', (error) => {
      clearTimeout(timeout);
      workerSlot.isAvailable = true;
      this.activeCount--;
      logger.error('Worker error:', error);
      reject(error);
      this.processQueue();
    });


    workerSlot.worker.postMessage(task);
  }


  processQueue() {
    if (this.taskQueue.length > 0 && this.workers.some(w => w.isAvailable)) {
      const { task, resolve, reject } = this.taskQueue.shift();
      const availableWorker = this.workers.find(w => w.isAvailable);
      this.executeOnWorker(availableWorker, task, resolve, reject);
    }
  }


  async terminate() {
    for (const workerSlot of this.workers) {
      if (workerSlot.worker) {
        try {
          await workerSlot.worker.terminate();
        } catch (err) {
          logger.warn('Error terminating worker:', err.message);
        }
      }
    }
    logger.info('Worker thread pool terminated');
  }
}



// ⭐ UPDATED: Use name-parser library for accurate name splitting
const parseFullName = (fullName) => {
  if (!fullName) return { first: '', last: '' };

  try {
    // Use name-parser library for accurate parsing
    const parsed = new Parser(fullName);
    const firstName = parsed.firstName() || '';
    const lastName = parsed.lastName() || '';

    // Validate that we got meaningful results
    if (!firstName && !lastName) {
      logger.warn(`name-parser couldn't parse: "${fullName}"`);
      // Fallback to manual split if library fails
      const parts = fullName.trim().split(/\s+/);
      return {
        first: parts[0] || '',
        last: parts.slice(1).join(' ') || ''
      };
    }

    // If one is missing but we have the other, use manual fallback for completeness
    if ((!firstName || !lastName) && fullName.trim()) {
      const parts = fullName.trim().split(/\s+/);
      return {
        first: firstName || parts[0] || '',
        last: lastName || parts.slice(1).join(' ') || ''
      };
    }

    return { first: firstName, last: lastName };
  } catch (error) {
    // logger.error(`Name parser error for "${fullName}":`, error.message);
    // Emergency fallback to manual parsing
    const parts = fullName.trim().split(/\s+/);
    return {
      first: parts[0] || '',
      last: parts.slice(1).join(' ') || ''
    };
  }
};

const extractEntitiesSimple = (document) => {
  const raw = document.entities || [];
  return raw.map((entity) => {
    // 1. Clean Value
    const value = String(entity.mentionText || entity.textAnchor?.content || '').trim();
    
    // 2. Get Type (Normalize to lowercase for safety)
    const type = (entity.type || 'text').toLowerCase().trim();

    // 3. Get Vertical Center (midY) for Row Grouping
    let midY = 0;
    try {
      const vertices = entity.pageAnchor?.pageRefs?.[0]?.boundingPoly?.normalizedVertices;
      if (vertices && vertices.length >= 4) {
        const ys = vertices.map(v => v.y);
        midY = (Math.min(...ys) + Math.max(...ys)) / 2;
      } else {
        // Without geometry, we can't place it in a row. Skip.
        return null;
      }
    } catch (e) { return null; }

    if (!value) return null;

    return { type, value, midY };
  }).filter(e => e !== null);
};


const simpleGrouping = (entities) => {
  if (!entities || entities.length === 0) return [];

  // --- 1. IDENTIFY ROW ANCHORS (NAMES) ---
  // We rely on the 'name' entity to be the "Spine" of the table.
  // Filter: Must be type 'name' AND have meaningful text (avoid icons/checkboxes)
  let anchors = entities
    .filter(e => (e.type === 'name' || e.type === 'person_name') && e.value.length > 2)
    .sort((a, b) => a.midY - b.midY);

  if (anchors.length === 0) {
    // Fallback: If model didn't label names, we can't group by row.
    // You might check for 'address' anchors here if names are missing, 
    // but you stated names are present in all records.
    logger.warn("No 'name' entities found. Grouping failed.");
    return [];
  }

  // --- 2. DEFINE VERTICAL ROW ZONES ---
  // We calculate the invisible horizontal lines that separate rows.
  // Rule: The boundary is exactly halfway between this Name and the next Name.
  const records = anchors.map((anchor, i) => {
    // Top Boundary
    let topY = 0;
    if (i > 0) {
      topY = (anchors[i - 1].midY + anchor.midY) / 2;
    }

    // Bottom Boundary
    let bottomY = 1.0;
    if (i < anchors.length - 1) {
      bottomY = (anchor.midY + anchors[i + 1].midY) / 2;
    }

    return {
      anchor,
      topY,
      bottomY,
      // The "Bag" to hold all entities for this person
      items: [] 
    };
  });

  // --- 3. DISTRIBUTE ENTITIES BY ROW (Y-AXIS ONLY) ---
  // We don't care about X columns. We only care: "Which row does this belong to?"
  entities.forEach(e => {
    // Skip the anchor itself to avoid duplication
    if (e === e.anchor) return; 

    // Find the row this entity sits inside
    const rec = records.find(r => e.midY >= r.topY && e.midY < r.bottomY);
    
    if (rec) {
      rec.items.push(e);
    }
  });

  // --- 4. MAP BY ENTITY TYPE ---
  return records.map(r => {
    // Helper: Find all items of a specific type in this row
    const getByType = (type) => r.items
      .filter(e => e.type === type)
      .map(e => e.value)
      .join(' ') // Join duplicates (e.g. multi-line address)
      .trim();

    // Name: We use the anchor's own value + any extra name parts found in the row
    const nameExtras = getByType('name'); 
    const fullName = r.anchor.value + (nameExtras ? ' ' + nameExtras : '');

    // Use your parser for splitting
    const { first, last } = parseFullName(fullName);

    return {
      first_name: first,
      last_name: last,
      // Map strictly by the DocAI label
      dateofbirth: getByType('date_of_birth') || getByType('dob') || getByType('dateofbirth'), 
      address: getByType('address'),
      mobile: getByType('mobile') || getByType('phone_number'), // Handle common aliases
      email: getByType('email'),
      landline: getByType('landline'),
      lastseen: getByType('last_seen') || getByType('lastseen')
    };
  });
};

const _single_line_address = (address) => {
  if (!address) return '';
  let s = address.replace(/\r/g, ' ').replace(/\n/g, ' ');
  s = s.replace(/[,;\|/]+/g, ' ');
  s = s.replace(REGEX_PATTERNS.whitespaceMultiple, ' ').trim();
  s = s.endsWith('.') ? s.slice(0, -1) : s;
  return s;
}


const fixAddressOrdering = (address) => {
  if (!address) return address;

  let s = _single_line_address(address).trim();
  let match;

  match = s.match(REGEX_PATTERNS.addressStatePostcodeStart);
  if (match) {
    const [, state, postcode, rest] = match;
    const out = `${rest.trim()} ${state.toUpperCase()} ${postcode}`;
    return out.replace(REGEX_PATTERNS.whitespaceMultiple, ' ').trim();
  }

  match = s.match(REGEX_PATTERNS.addressPostcodeStateEnd);
  if (match) {
    const [, postcode, rest, state] = match;
    const out = `${rest.trim()} ${state.toUpperCase()} ${postcode}`;
    return out.replace(REGEX_PATTERNS.whitespaceMultiple, ' ').trim();
  }

  match = s.match(REGEX_PATTERNS.addressStatePostcodeMiddle);
  if (match) {
    const [, part1, state, postcode, part2] = match;
    const out = `${part1.trim()} ${part2.trim()} ${state.toUpperCase()} ${postcode}`;
    return out.replace(REGEX_PATTERNS.whitespaceMultiple, ' ').trim();
  }

  match = s.match(REGEX_PATTERNS.addressStatePostcodeAny);
  if (match) {
    const state = match[1].toUpperCase();
    const postcode = match[2];
    const rest = (s.substring(0, match.index) + s.substring(match.index + match[0].length)).trim();
    const out = `${rest.replace(REGEX_PATTERNS.whitespaceMultiple, ' ')} ${state} ${postcode}`;
    return out.trim();
  }

  return s;
};


const cleanName = (name) => {
  if (!name) return '';
  let s = name.trim();
  s = s.replace(REGEX_PATTERNS.nameSpecialChars, '');
  s = s.replace(/[\d?]+/g, '');
  s = s.replace(REGEX_PATTERNS.nameInvalidChars, '');
  s = s.replace(REGEX_PATTERNS.whitespaceMultiple, ' ').trim();
  const parts = s ? s.split(' ').map(p => p.charAt(0).toUpperCase() + p.slice(1)) : [];
  return parts.join(' ').trim();
};



const normalizeDateField = (dateStr) => {
  if (!dateStr) return '';
  let s = dateStr.trim();
  s = s.replace(REGEX_PATTERNS.dashNormalize, '-');
  s = s.replace(REGEX_PATTERNS.dashMultiple, '-');
  s = s.replace(REGEX_PATTERNS.dashTrim, '');
  s = s.replace(REGEX_PATTERNS.dateInvalidChars, '');
  s = s.replace(/\./g, '-');

  const match = s.match(REGEX_PATTERNS.dateFormat);
  if (match) {
    s = `${match[1]}-${match[2]}-${match[3]}`;
  }

  try {
    const dt = new Date(s);
    if (isNaN(dt.getTime())) return '';
    const year = dt.getFullYear();
    const month = String(dt.getMonth() + 1).padStart(2, '0');
    const day = String(dt.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  } catch (e) {
    return '';
  }
};



const isValidLandline = (landline) => {
  if (!landline) return false;
  const digits = landline.replace(REGEX_PATTERNS.digitOnly, '');
  return digits.length >= 10;
};



// --- OPTIMIZATION 4: Exponential Backoff with Rate Limit Checking ---
const retryWithBackoff = async (fn, maxRetries = RETRY_ATTEMPTS, initialDelay = INITIAL_BACKOFF_MS) => {
  let lastError;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      // PITFALL FIX: Check active requests before attempting
      // Use a dynamic cap based on runtime scaled workers when available, but never exceed MAX_CONCURRENT_REQUESTS or SAFE_MAX_WORKERS
      const dynamicCap = Math.min(MAX_CONCURRENT_REQUESTS, SAFE_MAX_WORKERS, currentScaledWorkers || MAX_CONCURRENT_REQUESTS);
      if (activeRequests >= dynamicCap) {
        const waitTime = Math.min(1000, 100 * activeRequests);
        await new Promise(resolve => setTimeout(resolve, waitTime));
      }

      activeRequests++;
      const result = await Promise.race([
        fn(),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error('Request timeout')), REQUEST_TIMEOUT_MS)
        )
      ]);
      activeRequests--;
      return result;

    } catch (error) {
      activeRequests--;
      lastError = error;

  const msg = (error && error.message) ? String(error.message) : '';
  const isRateLimit = error && (error.code === 429 || msg.includes('RESOURCE_EXHAUSTED') || msg.toLowerCase().includes('rate limit'));
  // Only treat explicit internal 'Request timeout' errors as retryable timeouts.
  // Avoid broad 'timeout' substring matches that may misclassify unrelated errors.
  const isTimeout = msg === 'Request timeout' || msg.toLowerCase() === 'request timeout' || msg.includes('Request timeout');

      // Retry on rate limits or timeouts with exponential backoff
      if ((isRateLimit || isTimeout) && attempt < maxRetries - 1) {
        const delay = initialDelay * Math.pow(2, attempt);
        logger.warn(`${isRateLimit ? 'Rate limited' : 'Request timeout'}. Retrying after ${delay}ms (attempt ${attempt + 1}/${maxRetries})`);
        await new Promise(resolve => setTimeout(resolve, delay));
      } else if (!isRateLimit && !isTimeout) {
        // Non-retryable error — rethrow immediately
        throw error;
      }
    }
  }

  throw lastError;
};



// --- OPTIMIZATION 2: Batch Database Inserts ---
export const batchInsertRecords = async (records, dbClient, batchSize = BATCH_SIZE_RECORDS) => {
  if (!records || records.length === 0) {
    logger.debug('No records to insert');
    return { insertedCount: 0, batches: 0 };
  }

  let insertedCount = 0;
  let batchCount = 0;

  try {
    for (let i = 0; i < records.length; i += batchSize) {
      const batch = records.slice(i, i + batchSize);
      // Protect against PostgreSQL parameter limits: estimate params = rows * columns_per_row
      const columnsPerRow = (batch[0] && typeof batch[0] === 'object') ? Object.keys(batch[0]).length : 8;
      const PARAM_LIMIT = 60000;
      let maxRowsPerInsert = Math.floor(PARAM_LIMIT / Math.max(columnsPerRow, 1));
      if (maxRowsPerInsert < 1) maxRowsPerInsert = 1;

      if (batch.length > maxRowsPerInsert) {
        logger.warn(`Batch of ${batch.length} rows would exceed DB parameter limit (${columnsPerRow} cols * rows > ${PARAM_LIMIT}). Splitting into chunks of ${maxRowsPerInsert}.`);
      }

      // If needed, split the current batch into safe-sized sub-batches
      const subBatches = [];
      for (let j = 0; j < batch.length; j += maxRowsPerInsert) {
        subBatches.push(batch.slice(j, j + maxRowsPerInsert));
      }

      for (const subBatch of subBatches) {
        batchCount++;

        if (dbClient && typeof dbClient.insertBatch === 'function') {
          const result = await dbClient.insertBatch(subBatch);
          insertedCount += result.rowCount || subBatch.length;
        } else if (dbClient && typeof dbClient.collection === 'function') {
          const result = await dbClient.collection('records').insertMany(subBatch);
          insertedCount += result.insertedCount;
        }

        logger.debug(`Batch ${batchCount}: Inserted ${subBatch.length} records`);
      }
    }
    logger.info(`Total inserted: ${insertedCount} records in ${batchCount} batches`);
    return { insertedCount, batches: batchCount };
  } catch (error) {
    logger.error('Error in batch insert:', error.message);
    throw error;
  }
};



// --- OPTIMIZATION 3 & 7: Async File Operations ---
const readFileBuffered = async (tempPath) => {
  try {
    return await fsPromises.readFile(tempPath);
  } catch (error) {
    logger.error(`Failed to read file ${tempPath}:`, error.message);
    throw error;
  }
};



const cleanupTempFile = async (tempPath) => {
  try {
    await fsPromises.unlink(tempPath);
  } catch (error) {
    // PITFALL FIX: Non-fatal error handling
    if (error.code !== 'ENOENT') {
      logger.warn(`Failed to cleanup temp file ${tempPath}:`, error.message);
    }
  }
};



// --- OPTIMIZATION 5: Parallel JSON Generation ---
const generateJsonObjects = async (rawRecords, filteredRecords, entities, rawText, fileName) => {
  const [preProcessingJson, postProcessingJson] = await Promise.all([
    // Pre-processing JSON
    (async () => {
      const preProcessingRecords = rawRecords.map(record => ({
        full_name: `${record.first_name || ''} ${record.last_name || ''}`.trim(),
        dateofbirth: record.dateofbirth,
        address: record.address,
        mobile: record.mobile,
        email: record.email,
        landline: record.landline,
        lastseen: record.lastseen,
        file_name: record.file_name
      }));

      return {
        file_name: fileName,
        processing_timestamp: new Date().toISOString(),
        raw_records: preProcessingRecords,
        document_ai_entities: entities,
        total_entities: entities.length,
        entity_types: [...new Set(entities.map(e => e.type))],
        raw_text: rawText,
        metadata: {
          processor_id: config.processorId,
          project_id: config.projectId,
          location: config.location
        }
      };
    })(),

    // Post-processing JSON
    (async () => {
      return {
        file_name: fileName,
        processing_timestamp: new Date().toISOString(),
        raw_records: rawRecords,
        filtered_records: filteredRecords,
        summary: {
          total_raw_records: rawRecords.length,
          total_filtered_records: filteredRecords.length,
          success_rate: rawRecords.length > 0 ? `${((filteredRecords.length / rawRecords.length) * 100).toFixed(1)}%` : "0%"
        },
        field_counts: {
          names: rawRecords.filter(r => r.first_name).length,
          dateofbirths: rawRecords.filter(r => r.dateofbirth).length,
          addresses: rawRecords.filter(r => r.address).length,
          mobiles: rawRecords.filter(r => r.mobile).length,
          emails: rawRecords.filter(r => r.email).length,
          landlines: rawRecords.filter(r => r.landline).length,
          lastseens: rawRecords.filter(r => r.lastseen).length
        },
        metadata: {
          processor_id: config.processorId,
          project_id: config.projectId,
          location: config.location
        }
      };
    })()
  ]);

  return { preProcessingJson, postProcessingJson };
};



// --- OPTIMIZATION 5: Batch Record Processing in Parallel ---
const batchValidateRecords = async (records, batchSize = 100) => {
  // ✅ PRE-NORMALIZE addresses (and trim strings) BEFORE any validation/worker
  const prepped = records.map(r => ({
    ...r,
    first_name: String(r.first_name ?? '').trim(),
    last_name: String(r.last_name ?? '').trim(),
    dateofbirth: String(r.dateofbirth ?? '').trim(),
    lastseen: String(r.lastseen ?? '').trim(),
    mobile: String(r.mobile ?? '').trim(),
    email: String(r.email ?? '').trim(),
    landline: String(r.landline ?? '').trim(),
    // 👇 ensure address is reordered before any “starts-with-number” checks
    address: fixAddressOrdering(String(r.address ?? '').trim()),
  }));

  if (prepped.length <= batchSize || !workerThreadPool) {
    // Fall back to main thread validation if too small or no worker pool
    return cleanAndValidate(prepped);
  }

  const batches = [];
  for (let i = 0; i < prepped.length; i += batchSize) {
    batches.push(prepped.slice(i, i + batchSize));
  }

  try {
    const validatedBatches = await Promise.all(
      batches.map(batch =>
        workerThreadPool.runTask({
          type: 'validate',
          // ✅ send already-normalized records to the worker
          records: batch,
          patterns: REGEX_PATTERNS,
        })
      )
    );

    const allValid = validatedBatches.flatMap(b => (b && b.validRecords) ? b.validRecords : []);
    const allRejected = validatedBatches.flatMap(b => (b && b.rejectedRecords) ? b.rejectedRecords : []);
    return { validRecords: allValid, rejectedRecords: allRejected };
  } catch (error) {
    logger.warn('Worker thread validation failed, falling back to main thread:', error.message);
    return cleanAndValidate(prepped);
  }
};



// ⭐ UPDATED: Use safe String().trim() for all field access
const cleanAndValidate = (records) => {
  const cleanRecords = [];
  const rejectedRecords = [];

  for (const record of records) {
    // ⭐ Use String() to safely convert any type to string before trim()
    const rawFirst = String(record.first_name || '').trim();
    const rawLast = String(record.last_name || '').trim();
    const rawDob = String(record.dateofbirth || '').trim();
    const rawLastseen = String(record.lastseen || '').trim();

    const firstName = cleanName(rawFirst);
    const lastName = cleanName(rawLast);

    const mobile = String(record.mobile || '').trim();
    let address = String(record.address || '').trim();
    const email = String(record.email || '').trim();
    const rawLandline = String(record.landline || '').trim();

    address = fixAddressOrdering(address);

    const dateofbirth = normalizeDateField(rawDob);
    const lastseen = normalizeDateField(rawLastseen);

    if (!firstName || firstName.length <= 1) {
      rejectedRecords.push({
        first_name: firstName,
        last_name: lastName,
        mobile,
        address,
        email,
        dateofbirth,
        landline: rawLandline,
        lastseen,
        rejection_reason: 'Invalid first name (single character)'
      });
      continue;
    }

    if (!mobile) {
      rejectedRecords.push({
        first_name: firstName,
        last_name: lastName,
        mobile,
        address,
        email,
        dateofbirth,
        landline: rawLandline,
        lastseen,
        rejection_reason: 'Missing mobile number'
      });
      continue;
    }

    const mobileDigits = mobile.replace(REGEX_PATTERNS.digitOnly, '');
    if (!(mobileDigits.length === 10 && mobileDigits.startsWith('04'))) {
      rejectedRecords.push({
        first_name: firstName,
        last_name: lastName,
        mobile,
        address,
        email,
        dateofbirth,
        landline: rawLandline,
        lastseen,
        rejection_reason: 'Invalid mobile number'
      });
      continue;
    }

    const landline = isValidLandline(rawLandline) ? rawLandline.replace(REGEX_PATTERNS.digitOnly, '') : '';
    const full_name = `${firstName} ${lastName}`.trim();

    cleanRecords.push({
      full_name: full_name,
      first_name: firstName,
      last_name: lastName,
      dateofbirth: dateofbirth || '',
      address: address,
      mobile: mobileDigits,
      email: email || '',
      landline: landline,
      lastseen: lastseen || '',
    });
  }

  // De-duplication has been moved to processPDFs
  return { validRecords: cleanRecords, rejectedRecords };
};


// --- PDF Size Checking (PITFALL FIX) ---
const checkPdfSize = async (filePath, fileName) => {
  try {
    const stats = await fsPromises.stat(filePath);
    const fileSizeMB = stats.size / (1024 * 1024);

    if (stats.size > MAX_PDF_SIZE_BYTES) {
      throw new Error(`PDF exceeds max size (${fileSizeMB.toFixed(1)}MB > 50MB)`);
    }

    if (stats.size > PDF_SIZE_WARN_BYTES) {
      logger.warn(`Large PDF detected: ${fileName} (${fileSizeMB.toFixed(1)}MB)`);
    }

    return true;
  } catch (error) {
    throw error;
  }
};



// --- Graceful Shutdown Handler ---
const setupGracefulShutdown = async () => {
  const cleanup = async () => {
    logger.info('Shutting down gracefully...');
    if (workerThreadPool) {
      await workerThreadPool.terminate();
    }
    process.exit(0);
  };

  process.on('SIGINT', cleanup);
  process.on('SIGTERM', cleanup);

  // Handle uncaught exceptions in promises
  process.on('unhandledRejection', (reason, promise) => {
    logger.error('Unhandled Rejection at:', { promise, reason });
  });
};



// --- MAIN PROCESSING FUNCTION (Backward Compatible) ---


/**
 * Process PDFs with all optimizations + GCP safeguards
 * BACKWARD COMPATIBLE: Same interface as original
 * 
 * @param {Array} pdfFiles - Array of PDF file objects
 * @param {Number} batchSize - Unused (kept for compatibility)
 * @param {Number} maxWorkers - Unused (kept for compatibility, auto-determined)
 * @returns {Promise<Object>} Aggregated results from all files
 */
export const processPDFs = async (pdfFiles, batchSize = 10, maxWorkers = 4) => {
  // Initialize worker thread pool if needed
  // Initialize worker thread pool if needed (use a safe base pool size)
  const initialPoolSize = Math.min(BASE_WORKER_THREAD_POOL, SAFE_MAX_WORKERS);
  if (!workerThreadPool && pdfFiles.length >= 2) {
    workerThreadPool = new WorkerThreadPool(initialPoolSize);
    setupGracefulShutdown();
  }

  // Determine concurrency based on file count with aggressive scaling for high-resource environments
  const determineScaledWorkers = (count) => {
    // Aggressive scaling targets for 8 vGPU / 64GB environments
    if (count <= 1) return 4; // small jobs get a handful of workers
    if (count === 2) return 8;
    if (count <= 10) return 10;
    if (count <= 30) return Math.min(count * 2, 50); // medium batches scale up to 50
    if (count < 100) return 80; // counts less than 100 use 80
    if (count === 100) return 120; // exactly 100 files should use 120 workers
    return 120; // counts greater than 100 use 120 (within SAFE_MAX_WORKERS=150)
  };

  const scaledWorkers = determineScaledWorkers(pdfFiles.length);
  // Defensive clamp against SAFE_MAX_WORKERS to future-proof deployments
  const cappedWorkers = Math.min(scaledWorkers, SAFE_MAX_WORKERS);
  // Expose current scaled workers to retry/backoff logic — use the capped value so backoff gating matches p-limit
  currentScaledWorkers = cappedWorkers;
  const effectiveRequestCap = Math.min(MAX_CONCURRENT_REQUESTS, SAFE_MAX_WORKERS, cappedWorkers);
  logger.info(`[HIGH-PERFORMANCE MODE] Processing ${pdfFiles.length} files | Requested Workers: ${scaledWorkers} | Capped Workers: ${cappedWorkers} | Effective Request Cap: ${effectiveRequestCap} | Validation Thread Pool: ${initialPoolSize} threads | Max Capacity: ${SAFE_MAX_WORKERS}`);
  const limit = pLimit(cappedWorkers); // dynamic concurrency but capped by SAFE_MAX_WORKERS
  const startTime = Date.now();

  try {
    const tempDir = path.join(process.cwd(), "temp");
    if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir, { recursive: true });

    const processFile = async (file, index) => {
      const tempPath = path.join(tempDir, file.name);

      try {
        // Save uploaded file to temp
        await file.mv(tempPath);

        // PITFALL FIX: Check PDF size before processing
        await checkPdfSize(tempPath, file.name);

        // OPTIMIZATION 4: Retry with backoff
        const [result] = await retryWithBackoff(async () => {
          return await client.processDocument({
            name: `projects/${config.projectId}/locations/${config.location}/processors/${config.processorId}`,
            rawDocument: {
              content: await readFileBuffered(tempPath),
              mimeType: "application/pdf",
            },
          });
        }, RETRY_ATTEMPTS, INITIAL_BACKOFF_MS);

        const entities = extractEntitiesSimple(result.document);
        const rawRecords = simpleGrouping(entities);

        // OPTIMIZATION 5: Batch validate records in parallel (worker returns both valid and rejected)
        const { validRecords: filteredRecordsRaw, rejectedRecords: validationRejected } = await batchValidateRecords(rawRecords, 100);

        // 👇 --- DEDUPLICATION BLOCK (also track duplicates as rejected) --- 👇
        const uniqueRecords = [];
        const seenMobiles = new Set();
        const duplicateRejected = [];
        for (const record of (filteredRecordsRaw || [])) {
          if (!seenMobiles.has(record.mobile)) {
            uniqueRecords.push(record);
            seenMobiles.add(record.mobile);
          } else {
            duplicateRejected.push({
              ...record,
              rejection_reason: 'Duplicate mobile number'
            });
          }
        }
        const filteredRecords = uniqueRecords; // Use the de-duplicated list
        // 👆 --- END DEDUPLICATION --- 👆
        logger.debug('Extracted entities', { entities: entities.map(e => ({ type: e.type, value: e.value.substring(0, 30), startIndex: e.startIndex })) });
        const logData = JSON.stringify(entities.map(e => ({ type: e.type, value: e.value.substring(0, 30), startIndex: e.startIndex })), null, 2);
        try {
          // Only write entity debug files when verbose debugging is enabled to avoid I/O overhead
          if ((process.env.LOG_LEVEL || '').toLowerCase() === 'debug') {
            const safeName = String(file.name || 'unknown').replace(/[^a-z0-9_.-]/gi, '_');
            const outName = `./entity-debug-${safeName}-${Date.now()}.json`;
            fs.writeFileSync(outName, logData);
            logger.debug('Entities written to', outName);
          }
        } catch (e) {
          logger.warn('Failed to write entity debug file', e && e.message);
        }


  // Assign file name to all record types
  rawRecords.forEach(r => r.file_name = file.name);
  filteredRecords.forEach(r => r.file_name = file.name); // This now uses the unique list

  const allRejectedForFile = [...(validationRejected || []), ...duplicateRejected];
  allRejectedForFile.forEach(r => r.file_name = file.name);

        // OPTIMIZATION 5: Parallel JSON generation
        const { preProcessingJson, postProcessingJson } = await generateJsonObjects(
          rawRecords,
          filteredRecords,
          entities,
          result.document.text,
          file.name
        );

  logger.info(`[${index + 1}/${pdfFiles.length}] ${file.name} → ${filteredRecords.length} records`);

        return {
          rawRecords,
          filteredRecords,
          rejectedRecords: allRejectedForFile,
          preProcessingJson,
          postProcessingJson,
        };
      } catch (fileError) {
        logger.error(`Error processing file ${file.name}`, fileError && fileError.message);
        return {
          rawRecords: [],
          filteredRecords: [],
          rejectedRecords: [],
          preProcessingJson: null,
          postProcessingJson: null,
          error: fileError.message
        };
      } finally {
        // OPTIMIZATION 3: Async cleanup
        await cleanupTempFile(tempPath);
      }
    };

    // Process files with concurrency limit
    const processingPromises = pdfFiles.map((file, index) =>
      limit(() => processFile(file, index))
    );

    const results = await Promise.all(processingPromises);

    // Aggregate results
    const allRawRecords = results
      .filter(r => !r.error)
      .flatMap(r => r.rawRecords);

    const allFilteredRecords = results
      .filter(r => !r.error)
      .flatMap(r => r.filteredRecords);

    const allPreProcessingJson = results
      .filter(r => r.preProcessingJson)
      .map(r => r.preProcessingJson);

    const allPostProcessingJson = results
      .filter(r => r.postProcessingJson)
      .map(r => r.postProcessingJson);

    // Aggregate removed/rejected records from each file
    const allRemovedRecordsRaw = results
      .filter(r => !r.error)
      .flatMap(r => r.rejectedRecords || []);

    const allRemovedRecords = allRemovedRecordsRaw.map((record, index) => ({
      id: index + 1,
      full_name: `${record.first_name || ''} ${record.last_name || ''}`.trim(),
      file_name: record.file_name,
      rejection_reason: record.rejection_reason
    }));

    const processingTime = ((Date.now() - startTime) / 1000).toFixed(2);
    const successRate = allRawRecords.length > 0
      ? `${((allFilteredRecords.length / allRawRecords.length) * 100).toFixed(1)}%`
      : "0%";

  logger.info(`Processing complete in ${processingTime}s | Success rate: ${successRate}`);

    // PITFALL FIX: Cleanup worker pool after batch
    if (workerThreadPool && pdfFiles.length >= 10 && activeRequests === 0) {
  // Keep pool alive for reuse
  logger.info('Worker pool ready for reuse');
    }

    return {
      allRawRecords,
      allFilteredRecords,
      allRemovedRecords,
      allPreProcessingJson,
      allPostProcessingJson,
    };
  } catch (error) {
    logger.error("Error in processPDFs:", error);
    throw error;
  }
};



// --- Cleanup on module unload ---
process.on('exit', async () => {
  if (workerThreadPool) {
    await workerThreadPool.terminate();
  }
});


// export { batchInsertRecords };