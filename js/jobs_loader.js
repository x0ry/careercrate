// ============================================================
// JOBS LOADER
// ============================================================

import { showToast } from "./ui_utils.js";
import {
    getCacheMeta,
    setCacheMeta,
    putChunk,
    getAllJobsForVersion,
    deleteVersion,
} from "./idb_cache.js";

/**
 * Load all job chunks, preferring a complete on-disk (IndexedDB) copy of the
 * current manifest version over the network. On a cache hit the full
 * dataset is available immediately - no gzip download or JSON.parse of
 * ~1.4M records repeated on every visit. On a cache miss, chunks are
 * fetched in parallel via a worker (instead of one-at-a-time) and persisted
 * to IndexedDB as they arrive, so an interrupted session still leaves
 * usable data behind for next time, and a fully-fetched dataset serves
 * instantly on the next reload.
 *
 * Resolves once the first bit of data is renderable (either the whole
 * cached dataset, or the first network chunk to arrive) - remaining chunks
 * on the network path continue streaming in the background exactly as
 * before.
 *
 * @param {object} app
 * @param {string} basePath
 */
export async function loadJobsProgressive(app, basePath = 'data/chunks') {
    const thead = document.querySelector('.job-table thead');
    thead?.classList.add('sorting-locked');

    // Resolve to an absolute URL up front - a relative basePath resolves
    // fine on the main thread (against the page's own URL), but the chunk
    // worker fetches chunk URLs from inside js/chunk_worker.js, where a
    // relative path would resolve against the worker's own script location
    // instead and 404.
    basePath = new URL(basePath, document.baseURI).href;

    const manifest = await fetch(`${basePath}/jobs_manifest.json?t=${Date.now()}`).then(res => {
        if (!res.ok) throw new Error('Failed to load jobs manifest');
        return res.json();
    });

    const version = manifest.last_updated;
    const chunkCount = manifest.chunks.length;

    // Always spin up a worker so sorting is offloaded even on the cache-hit
    // path, whether or not it ends up fetching anything over the network.
    const worker = new Worker('./js/chunk_worker.js', { type: 'module' });
    app.sortWorker = worker;

    // ── Fast path: a complete, current snapshot already lives in IndexedDB ──
    const meta = await getCacheMeta();
    if (meta?.complete && meta.version === version) {
        const cachedJobs = await getAllJobsForVersion(version, chunkCount);
        if (cachedJobs) {
            app.allJobs = cachedJobs;
            app.filteredJobs = cachedJobs;
            app.isFullyLoaded = true;
            updateStats(app.allJobs, manifest.last_updated);
            app.render();
            thead?.classList.remove('sorting-locked');
            attachSortHandler(app, worker);
            console.log(`Loaded ${cachedJobs.length.toLocaleString()} jobs instantly from cache (version ${version}).`);
            return;
        }
    }

    // ── Slow path: fetch every chunk from the network in parallel ──
    try {
        await fetchAllChunksFromNetwork(app, worker, manifest, basePath, version, meta);
    } catch (err) {
        // Total failure (e.g. the very first chunk to resolve errored out and
        // nothing else came in behind it) - fall back to the last complete
        // cached snapshot rather than showing the user nothing.
        const fallbackVersion = meta?.complete ? meta.version : null;
        const fallbackJobs = fallbackVersion
            ? await getAllJobsForVersion(fallbackVersion, meta.chunkCount)
            : null;

        if (fallbackJobs) {
            app.allJobs = fallbackJobs;
            app.filteredJobs = fallbackJobs;
            app.isFullyLoaded = true;
            updateStats(app.allJobs, fallbackVersion);
            app.render();
            thead?.classList.remove('sorting-locked');
            attachSortHandler(app, worker);
            showToast('Could not reach the latest job data - showing your last cached results instead.', 'warning');
            return;
        }

        thead?.classList.remove('sorting-locked');
        throw err;
    }
}

/**
 * Dispatch every chunk fetch to the worker in parallel, render/accumulate
 * as results land (in whatever order they arrive), persist each chunk to
 * IndexedDB, and finalize the cache once the full set has landed.
 * Resolves as soon as the first chunk is renderable.
 */
function fetchAllChunksFromNetwork(app, worker, manifest, basePath, version, previousMeta) {
    const thead = document.querySelector('.job-table thead');
    const v = encodeURIComponent(version);
    const chunkCount = manifest.chunks.length;

    app.allJobs = [];
    app.filteredJobs = [];

    let pending = chunkCount;
    let succeeded = 0;
    let firstRenderDone = false;
    let settleFirstRender;
    let rejectAll;

    const firstRenderPromise = new Promise((resolve, reject) => {
        settleFirstRender = resolve;
        rejectAll = reject;
    });

    const finishIfDone = () => {
        if (pending > 0) return;

        app.isFullyLoaded = true;
        thead?.classList.remove('sorting-locked');

        if (succeeded === chunkCount) {
            setCacheMeta({ version, totalJobs: manifest.totalJobs, chunkCount, complete: true });
            if (previousMeta?.version && previousMeta.version !== version) {
                deleteVersion(previousMeta.version);
            }
        } else if (succeeded > 0) {
            showToast(`${chunkCount - succeeded} chunk(s) failed to load - results may be incomplete.`, 'warning');
        }

        if (!firstRenderDone) {
            // every chunk failed - nothing was ever rendered
            rejectAll(new Error('All job chunks failed to load'));
        }
    };

    worker.onmessage = ({ data }) => {
        if (data.type === 'CHUNK_LOADED') {
            succeeded++;
            putChunk(version, data.chunkIndex, data.jobsChunk);

            if (!firstRenderDone) {
                firstRenderDone = true;
                app.allJobs = [...data.jobsChunk];
                app.filteredJobs = app.allJobs;
                updateStats(app.allJobs, manifest.last_updated);
                app.render();
                thead?.classList.remove('sorting-locked');
                settleFirstRender();
            } else {
                app.allJobs.push(...data.jobsChunk);
                app.refilter();
                updateStats(app.allJobs, manifest.last_updated);
            }

            pending--;
            finishIfDone();
        }

        if (data.type === 'CHUNK_ERROR') {
            console.error(`Chunk ${data.chunkIndex} failed:`, data.message);
            pending--;
            finishIfDone();
        }

        handleSortMessage(app, data);
    };

    manifest.chunks.forEach((chunk, chunkIndex) => {
        worker.postMessage({ type: 'FETCH_CHUNK', url: `${basePath}/${chunk}?v=${v}`, chunkIndex });
    });

    return firstRenderPromise;
}

/** Registers the SORT/SORT_ERROR handler on a worker that skipped fetchAllChunksFromNetwork. */
function attachSortHandler(app, worker) {
    worker.onmessage = ({ data }) => handleSortMessage(app, data);
}

function handleSortMessage(app, data) {
    if (data.type === 'SORTED') {
        app.sortedJobs = data.sortedJobs;
        app.virtualFilteredCount = data.sortedJobs.length;
        if (app.sortLoader?.hide) app.sortLoader.hide();
        app.isSorting = false;
        app.render();
    }
    if (data.type === 'SORT_ERROR') {
        console.error('Worker sort failed:', data.message);
        if (app.sortLoader?.hide) app.sortLoader.hide();
        app.isSorting = false;
    }
}

/**
 * Update the stats bar in the DOM.
 * @param {Array} jobs - The full jobs array
 * @param {string} [lastUpdated] - ISO timestamp from manifest
 */
export function updateStats(jobs, lastUpdated) {
    const companies = new Set(jobs.map(j => j.company_slug || j.company)).size;
    document.getElementById('total-jobs').textContent = jobs.length.toLocaleString();
    document.getElementById('total-companies').textContent = companies.toLocaleString();
    document.getElementById('last-updated').textContent = lastUpdated
        ? new Date(lastUpdated).toLocaleDateString()
        : new Date().toLocaleDateString();
}
