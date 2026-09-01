// ============================================================
// PERSISTENT JOB DATA CACHE (IndexedDB)
// ============================================================
//
// Job chunks are large (tens of MB each once decompressed) and are
// re-downloaded + gunzipped + JSON.parse'd from the CDN on every page
// load. That's the real cost behind "jobs trickling in slowly" - not
// just network time, but redoing the same decompression/parse work
// every single visit.
//
// This module persists each parsed chunk to IndexedDB, keyed by the
// manifest's `last_updated` version string, as soon as it arrives.
// A later page load whose manifest version matches what's cached can
// skip the network entirely and render straight from disk. If a
// version doesn't finish downloading (tab closed mid-load, a chunk
// 404s, etc.) it's never marked `complete`, so a half-finished cache
// is never mistaken for the full dataset - it just gets resumed/redone
// next time, while any previous *complete* version stays available as
// a fallback.
//
// All operations fail soft (return null / no-op) if IndexedDB is
// unavailable (private browsing, disabled storage, quota errors) so
// this is always safe to call speculatively.

const DB_NAME = 'careercrate-jobs';
const DB_VERSION = 1;
const CHUNKS_STORE = 'chunks';
const META_STORE = 'meta';
const META_KEY = 'singleton';

let dbPromise = null;

function openDB() {
    if (dbPromise) return dbPromise;
    if (typeof indexedDB === 'undefined') return Promise.resolve(null);

    dbPromise = new Promise((resolve) => {
        let request;
        try {
            request = indexedDB.open(DB_NAME, DB_VERSION);
        } catch {
            resolve(null);
            return;
        }

        request.onupgradeneeded = () => {
            const db = request.result;
            if (!db.objectStoreNames.contains(CHUNKS_STORE)) {
                const store = db.createObjectStore(CHUNKS_STORE, { keyPath: 'id' });
                store.createIndex('byVersion', 'version', { unique: false });
            }
            if (!db.objectStoreNames.contains(META_STORE)) {
                db.createObjectStore(META_STORE, { keyPath: 'key' });
            }
        };

        request.onsuccess = () => resolve(request.result);
        request.onerror = () => resolve(null);
        request.onblocked = () => resolve(null);
    });

    return dbPromise;
}

/** Read the cache's metadata record (version, completeness, chunk count). */
export async function getCacheMeta() {
    const db = await openDB();
    if (!db) return null;

    return new Promise((resolve) => {
        try {
            const tx = db.transaction(META_STORE, 'readonly');
            const req = tx.objectStore(META_STORE).get(META_KEY);
            req.onsuccess = () => resolve(req.result || null);
            req.onerror = () => resolve(null);
        } catch {
            resolve(null);
        }
    });
}

/** Write/replace the cache's metadata record. */
export async function setCacheMeta(meta) {
    const db = await openDB();
    if (!db) return;

    return new Promise((resolve) => {
        try {
            const tx = db.transaction(META_STORE, 'readwrite');
            tx.objectStore(META_STORE).put({ key: META_KEY, ...meta });
            tx.oncomplete = () => resolve();
            tx.onerror = () => resolve();
        } catch {
            resolve();
        }
    });
}

/** Persist one already-parsed chunk of jobs under a manifest version. */
export async function putChunk(version, chunkIndex, jobs) {
    const db = await openDB();
    if (!db) return;

    return new Promise((resolve) => {
        try {
            const tx = db.transaction(CHUNKS_STORE, 'readwrite');
            tx.objectStore(CHUNKS_STORE).put({
                id: `${version}#${chunkIndex}`,
                version,
                chunkIndex,
                jobs,
            });
            tx.oncomplete = () => resolve();
            tx.onerror = () => resolve();
        } catch {
            resolve();
        }
    });
}

/**
 * Load every cached chunk for a version, concatenated in chunk order.
 * Returns null (not []) if anything is missing/unreadable, so callers
 * can tell "no usable cache" apart from "cache is legitimately empty".
 */
export async function getAllJobsForVersion(version, expectedChunkCount) {
    const db = await openDB();
    if (!db) return null;

    const rows = await new Promise((resolve) => {
        try {
            const tx = db.transaction(CHUNKS_STORE, 'readonly');
            const index = tx.objectStore(CHUNKS_STORE).index('byVersion');
            const range = IDBKeyRange.only(version);
            const results = [];
            const req = index.openCursor(range);
            req.onsuccess = (e) => {
                const cursor = e.target.result;
                if (cursor) {
                    results.push(cursor.value);
                    cursor.continue();
                } else {
                    resolve(results);
                }
            };
            req.onerror = () => resolve(null);
        } catch {
            resolve(null);
        }
    });

    if (!rows) return null;
    if (expectedChunkCount != null && rows.length !== expectedChunkCount) return null;

    rows.sort((a, b) => a.chunkIndex - b.chunkIndex);
    const allJobs = [];
    for (const row of rows) allJobs.push(...row.jobs);
    return allJobs;
}

/** Delete every chunk belonging to a stale version (cleanup after a successful refresh). */
export async function deleteVersion(version) {
    const db = await openDB();
    if (!db) return;

    return new Promise((resolve) => {
        try {
            const tx = db.transaction(CHUNKS_STORE, 'readwrite');
            const index = tx.objectStore(CHUNKS_STORE).index('byVersion');
            const range = IDBKeyRange.only(version);
            const req = index.openCursor(range);
            req.onsuccess = (e) => {
                const cursor = e.target.result;
                if (cursor) {
                    cursor.delete();
                    cursor.continue();
                } else {
                    resolve();
                }
            };
            req.onerror = () => resolve();
        } catch {
            resolve();
        }
    });
}
