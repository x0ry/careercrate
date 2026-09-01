// ============================================================
// Chunk_Worker.js
// ============================================================
import { sortJobs } from './sort_logic.js';

self.onmessage = async ({ data }) => {
    // ── SORTING REQUEST ──────────────────────────────────────
    if (data.type === 'SORT') {
        try {
            const { jobsToSort, sortState } = data;
            const processed = sortJobs(jobsToSort, sortState);
            self.postMessage({ type: 'SORTED', sortedJobs: processed });
        } catch (err) {
            self.postMessage({ type: 'SORT_ERROR', message: String(err?.stack || err) });
        }
        return;
    }
    // ── CHUNK FETCHING REQUEST ───────────────────────────────
    if (data.type === 'FETCH_CHUNK') {
        try {
            const res = await fetch(data.url);
            if (!res.ok) throw new Error(`Failed to fetch ${data.url}`);
            const blob = await res.blob();
            const ds = new DecompressionStream('gzip');
            const text = await new Response(blob.stream().pipeThrough(ds)).blob().then(b => b.text());
            const newJobs = JSON.parse(text);
            self.postMessage({ type: 'CHUNK_LOADED', chunkIndex: data.chunkIndex, jobsChunk: newJobs });
        } catch (err) {
            self.postMessage({ type: 'CHUNK_ERROR', chunkIndex: data.chunkIndex, message: String(err?.stack || err) });
        }
    }
};