// ============================================================
// EVENT LISTENERS
// ============================================================

import { escape, showToast } from './ui_utils.js';
import { saveApplicationStatus, deleteApplicationStatus } from './storage.js';

const ACTION_CHECKBOXES = ['.save-checkbox', '.apply-checkbox', '.ignored-checkbox'];
const STATUS_BY_CLASS = {
    'save-checkbox': 'saved',
    'apply-checkbox': 'applied',
    'ignored-checkbox': 'ignored',
};
const STATUS_LABEL = { saved: 'Saved', applied: 'Applied', ignored: 'Ignored' };

/**
 * Wire up all DOM event listeners.
 * @param {object} app - The JobBoardApp instance
 */
export function setupEventListeners(app) {

    // ── Pagination (top + bottom) ────────────────────────────
    document.getElementById('prev-page').addEventListener('click', () => app.previousPage());
    document.getElementById('next-page').addEventListener('click', () => app.nextPage());
    document.getElementById('prev-page-bottom').addEventListener('click', () => app.previousPage());
    document.getElementById('next-page-bottom').addEventListener('click', () => app.nextPage());

    // ── Per-page selector ────────────────────────────────────
    document.getElementById('per-page').addEventListener('change', (e) => {
        app.perPage = parseInt(e.target.value);
        app.currentPage = 1;
        app.render();
    });

    // ── Filter buttons ───────────────────────────────────────
    document.getElementById('apply-filters').addEventListener('click', () => app.applyFilters());
    document.getElementById('clear-filters').addEventListener('click', () => app.clearFilters());

    // Enter key on text filter inputs
    ['filter-title', 'filter-company', 'filter-location', 'filter-salary-min',
        'filter-exclude', 'filter-include'].forEach(id => {
            document.getElementById(id).addEventListener('keypress', (e) => {
                if (e.key === 'Enter') app.applyFilters();
            });
        });

    // ── Sorting — table header clicks ────────────────────────
    document.querySelectorAll('.job-table thead th').forEach((th, index) => {
        const column = app.columns[index];
        if (column && column.sortable) {
            th.style.cursor = 'pointer';
            th.addEventListener('click', () => {
                if (!app.isFullyLoaded) return;

                app.handleSort(column.key);
            });
        }
    });

    // ── Mobile sort controls ─────────────────────────────────
    const mobileSortKey = document.getElementById('mobile-sort-key');
    const mobileSortDir = document.getElementById('mobile-sort-dir');

    if (mobileSortKey) {
        mobileSortKey.addEventListener('change', (e) => {
            if (e.target.value) {
                app.sortState.key = e.target.value;
                app.sortState.direction = 'asc';
                mobileSortDir.textContent = 'A-Z';
            } else {
                app.sortState.key = null;
            }
            app.currentPage = 1;
            app.render();
        });
    }

    if (mobileSortDir) {
        mobileSortDir.addEventListener('click', () => {
            if (!app.sortState.key) return;
            app.sortState.direction = app.sortState.direction === 'asc' ? 'desc' : 'asc';
            mobileSortDir.textContent = app.sortState.direction === 'asc' ? 'A-Z' : 'Z-A';
            app.currentPage = 1;
            app.sortAndRender();
        });
    }

    // ── Dropdown filters (instant apply) ─────────────────────
    document.getElementById('filter-status').addEventListener('change', () => app.applyFilters());
    document.getElementById('filter-ats').addEventListener('change', () => app.applyFilters());
    document.getElementById('filter-skill-level').addEventListener('change', () => app.applyFilters());
    document.getElementById('filter-hide-applied').addEventListener('change', () => app.applyFilters());

    // ── Delegated: status radio click commits immediately ──
    // Saved/Applied/Ignored is a radio group now (see columns.js) - the
    // browser already guarantees exactly one is active, so this just
    // persists whichever one fired. "Saved" is the implicit default for
    // any job with no record, so selecting it clears the record instead of
    // writing a redundant explicit one.
    document.addEventListener('change', (e) => {
        if (!e.target.matches(ACTION_CHECKBOXES.join(', '))) return;

        const jobUrl = e.target.dataset.jobUrl;
        const clickedClass = Object.keys(STATUS_BY_CLASS).find(cls => e.target.classList.contains(cls));
        const status = STATUS_BY_CLASS[clickedClass];

        if (status === 'saved') {
            deleteApplicationStatus(jobUrl);
        } else {
            saveApplicationStatus(jobUrl, status);
        }
        showToast(`Marked as ${STATUS_LABEL[status]}.`, 'success');
    });

    const filterCollapse = document.getElementById('filter-controls');
    const filterToggle = document.querySelector('.filter-toggle');

    filterCollapse.addEventListener('show.bs.collapse', () => {
        filterToggle.classList.add('open');
    });

    filterCollapse.addEventListener('hidden.bs.collapse', () => {
        filterToggle.classList.remove('open');
    });
}