// ============================================================
// LOCAL STORAGE UTILITIES
// ============================================================

const STORAGE_KEY = 'job-applications';

/** Load all application statuses from localStorage */
export function loadApplicationStatus() {
    try{
        const saved = localStorage.getItem(STORAGE_KEY);
        return saved ? JSON.parse(saved) : {};
    } catch{
        return {};
    }
}

/**
 * Resolve a job's effective status. A job with no explicit record is
 * "saved" by default - untouched jobs are implicitly saved, not statusless,
 * so this is the single source of truth for that default rather than every
 * caller repeating `?.status || 'saved'`.
 */
export function getJobStatus(jobUrl) {
    return loadApplicationStatus()[jobUrl]?.status || 'saved';
}

/** Save a job's application status */
export function saveApplicationStatus(jobUrl, status) {
    const apps = loadApplicationStatus();
    apps[jobUrl] = {
        status: status, // 'saved', 'applied', 'ignored'
        date: new Date().toISOString()
    };
    localStorage.setItem(STORAGE_KEY, JSON.stringify(apps));
}

/** Delete a job's application status */
export function deleteApplicationStatus(jobUrl) {
    const apps = loadApplicationStatus();
    delete apps[jobUrl];
    localStorage.setItem(STORAGE_KEY, JSON.stringify(apps));
}