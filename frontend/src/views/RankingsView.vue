<script setup>
import { onMounted, computed } from 'vue'
import { useRankingsStore } from '@/stores/rankings.js'
import { chainLabel, chainLogoUrl, DARK_BG_LOGOS } from '@/utils/chainLogos.js'

const rankings = useRankingsStore()

onMounted(() => rankings.loadRankings())

const BASE_URL = import.meta.env.BASE_URL

// Derive visible weeks from history (newest first, max 8)
const historyWeeks = computed(() => (rankings.history ?? []).slice(0, 8))

// Chains that appear in history for the historical table columns
const historyChains = computed(() => {
  const seen = new Set()
  for (const week of historyWeeks.value) {
    for (const c of week.chains ?? []) seen.add(c.store_chain)
  }
  return [...seen].sort()
})

// Build a lookup: { week_label: { store_chain: row } }
const historyIndex = computed(() => {
  const idx = {}
  for (const week of historyWeeks.value) {
    idx[week.week_label] = {}
    for (const row of week.chains ?? []) {
      idx[week.week_label][row.store_chain] = row
    }
  }
  return idx
})

function gradeColor(letter) {
  const map = { A: '#4caf73', B: '#f0a500', C: '#e9a23b', D: '#e05c4b', F: '#c0392b' }
  return map[letter] ?? 'rgba(244,239,224,0.3)'
}

function formatWeek(wl) {
  if (!wl) return ''
  // "2026-W20" → "Wk 20"
  const m = wl.match(/W(\d+)$/)
  return m ? `Wk ${m[1]}` : wl
}

function hotPct(row) {
  if (!row || !row.item_count) return '—'
  return `${Math.round((row.hot_ratio ?? 0) * 100)}%`
}
</script>

<template>
  <main class="rankings-view">
    <div class="rankings-inner">

      <!-- Header -->
      <div class="page-header">
        <h2 class="page-heading">
          Flyer Rankings
          <em>This Week</em>
        </h2>
        <p class="page-sub">
          Overall grade for each chain's weekly flyer, scored across all deal evaluations.
          Not geolocated — shows the national flyer lineup.
        </p>
      </div>

      <!-- Loading -->
      <div v-if="rankings.isLoading" class="state-block">
        <span class="spinner" />
        <span class="state-text">Loading rankings…</span>
      </div>

      <!-- Error -->
      <div v-else-if="rankings.error" class="state-block error">
        <span class="state-icon">⚠</span>
        <span class="state-text">{{ rankings.error }}</span>
        <p class="state-hint">
          Run <code>python -m pipeline.flyer_ranker</code> then
          <code>python scripts/export_frontend_data.py --rankings-only</code>
          to generate rankings data.
        </p>
      </div>

      <!-- No data -->
      <div v-else-if="rankings.chains.length === 0" class="state-block">
        <span class="state-icon">◈</span>
        <p class="state-text">No rankings data available yet.</p>
        <p class="state-hint">
          Run <code>python -m pipeline.flyer_ranker</code> then
          <code>python scripts/export_frontend_data.py --rankings-only</code>.
        </p>
      </div>

      <!-- Current rankings table -->
      <template v-else>
        <section class="section">
          <h3 class="section-heading">Chain Leaderboard</h3>
          <div class="chain-table-wrap">
            <table class="chain-table">
              <thead>
                <tr>
                  <th class="col-rank">#</th>
                  <th class="col-chain">Chain</th>
                  <th class="col-grade">Grade</th>
                  <th class="col-score">Avg Score</th>
                  <th class="col-hot">🔥 Hot</th>
                  <th class="col-items">Items</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  v-for="row in rankings.chains"
                  :key="row.store_chain"
                  class="chain-row"
                >
                  <td class="col-rank rank-num">{{ row.rank }}</td>
                  <td class="col-chain">
                    <span class="chain-name-cell">
                      <span
                        class="chain-logo-wrap"
                        :class="{ 'dark-bg': DARK_BG_LOGOS.has(row.store_chain) }"
                      >
                        <img
                          v-if="chainLogoUrl(row.store_chain)"
                          :src="`${BASE_URL}${chainLogoUrl(row.store_chain)}`"
                          :alt="chainLabel(row.store_chain)"
                          class="chain-logo"
                          @error="e => e.target.style.display='none'"
                        />
                      </span>
                      <span class="chain-label">{{ chainLabel(row.store_chain) }}</span>
                    </span>
                  </td>
                  <td class="col-grade">
                    <span
                      class="grade-badge"
                      :style="{ background: gradeColor(row.letter_grade), color: '#1b2d1e' }"
                    >{{ row.letter_grade }}</span>
                  </td>
                  <td class="col-score score-num">{{ row.avg_flyer_grade.toFixed(1) }}</td>
                  <td class="col-hot">
                    <span class="hot-bar-wrap">
                      <span
                        class="hot-bar"
                        :style="{ width: `${Math.round((row.hot_ratio ?? 0) * 100)}%` }"
                      />
                      <span class="hot-pct">{{ hotPct(row) }}</span>
                    </span>
                  </td>
                  <td class="col-items muted">{{ row.item_count.toLocaleString() }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>

        <!-- Historical rankings section -->
        <section v-if="historyWeeks.length > 1" class="section">
          <h3 class="section-heading">Historical Rankings</h3>
          <p class="section-sub">Chain grade by week — newest left. Up to 8 weeks shown.</p>
          <div class="history-table-wrap">
            <table class="history-table">
              <thead>
                <tr>
                  <th class="hcol-chain">Chain</th>
                  <th
                    v-for="week in historyWeeks"
                    :key="week.week_label"
                    class="hcol-week"
                  >{{ formatWeek(week.week_label) }}</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  v-for="chain in historyChains"
                  :key="chain"
                  class="history-row"
                >
                  <td class="hcol-chain chain-label-cell">
                    {{ chainLabel(chain) }}
                  </td>
                  <td
                    v-for="week in historyWeeks"
                    :key="week.week_label"
                    class="hcol-week grade-cell"
                  >
                    <template v-if="historyIndex[week.week_label]?.[chain]">
                      <span
                        class="grade-badge grade-badge-sm"
                        :style="{ background: gradeColor(historyIndex[week.week_label][chain].letter_grade), color: '#1b2d1e' }"
                      >{{ historyIndex[week.week_label][chain].letter_grade }}</span>
                      <span class="history-score">
                        {{ historyIndex[week.week_label][chain].avg_flyer_grade.toFixed(0) }}
                      </span>
                    </template>
                    <span v-else class="muted">—</span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>
      </template>
    </div>
  </main>
</template>

<style scoped>
.rankings-view {
  flex: 1;
  background: var(--c-bg);
  overflow-y: auto;
}

.rankings-inner {
  max-width: 960px;
  margin: 0 auto;
  padding: var(--space-xl) var(--space-lg);
  display: flex;
  flex-direction: column;
  gap: var(--space-xl);
}

/* ── Page header ─────────────────────────────────────── */
.page-header {
  border-bottom: 1px solid var(--c-border);
  padding-bottom: var(--space-md);
}

.page-heading {
  font-family: var(--font-display);
  font-size: 2.2rem;
  font-weight: 700;
  color: var(--c-ivory);
  margin: 0 0 8px;
  letter-spacing: -0.01em;
}

.page-heading em {
  font-style: italic;
  color: var(--c-amber);
}

.page-sub {
  font-family: var(--font-body);
  font-size: 0.9rem;
  line-height: 1.6;
  color: var(--c-muted);
  max-width: 580px;
}

/* ── Section ─────────────────────────────────────────── */
.section {
  display: flex;
  flex-direction: column;
  gap: var(--space-md);
}

.section-heading {
  font-family: var(--font-display);
  font-size: 1.4rem;
  font-weight: 600;
  color: var(--c-ivory);
  margin: 0;
  border-bottom: 1px solid var(--c-border);
  padding-bottom: var(--space-sm);
}

.section-sub {
  font-family: var(--font-body);
  font-size: 0.8rem;
  color: var(--c-muted);
  margin: -8px 0 0;
}

/* ── Chain leaderboard table ─────────────────────────── */
.chain-table-wrap {
  overflow-x: auto;
}

.chain-table {
  width: 100%;
  border-collapse: collapse;
  font-family: var(--font-body);
  font-size: 0.88rem;
}

.chain-table thead tr {
  border-bottom: 1px solid var(--c-border);
}

.chain-table th {
  text-align: left;
  padding: 8px 12px;
  font-size: 0.72rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--c-muted);
  font-weight: 400;
  white-space: nowrap;
}

.chain-row {
  border-bottom: 1px solid rgba(244, 239, 224, 0.05);
  transition: background 0.15s;
}

.chain-row:hover {
  background: var(--c-surface);
}

.chain-table td {
  padding: 12px 12px;
  vertical-align: middle;
}

.col-rank { width: 36px; }
.col-chain { min-width: 180px; }
.col-grade { width: 70px; }
.col-score { width: 90px; }
.col-hot   { min-width: 110px; }
.col-items { width: 70px; }

.rank-num {
  font-family: var(--font-display);
  font-size: 1.3rem;
  font-weight: 700;
  color: rgba(244, 239, 224, 0.35);
}

.chain-name-cell {
  display: flex;
  align-items: center;
  gap: 10px;
}

.chain-logo-wrap {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 44px;
  height: 24px;
  background: #fff;
  border-radius: 3px;
  padding: 2px 4px;
  flex-shrink: 0;
}

.chain-logo-wrap.dark-bg {
  background: #1b2d1e;
  border: 1px solid rgba(255, 255, 255, 0.15);
}

.chain-logo {
  max-height: 18px;
  max-width: 40px;
  object-fit: contain;
}

.chain-label {
  font-family: var(--font-body);
  color: var(--c-ivory);
  font-size: 0.9rem;
}

.grade-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  border-radius: 4px;
  font-family: var(--font-display);
  font-size: 1.1rem;
  font-weight: 700;
  line-height: 1;
}

.grade-badge-sm {
  width: 24px;
  height: 24px;
  font-size: 0.85rem;
}

.score-num {
  font-family: var(--font-display);
  font-size: 1.1rem;
  font-weight: 600;
  color: var(--c-ivory);
}

.hot-bar-wrap {
  display: flex;
  align-items: center;
  gap: 8px;
}

.hot-bar {
  display: block;
  height: 6px;
  background: #e74c3c;
  border-radius: 3px;
  min-width: 2px;
  max-width: 60px;
  flex-shrink: 0;
  transition: width 0.4s ease;
}

.hot-pct {
  font-size: 0.8rem;
  color: var(--c-muted);
  white-space: nowrap;
}

.muted {
  color: var(--c-muted);
}

/* ── Historical table ────────────────────────────────── */
.history-table-wrap {
  overflow-x: auto;
}

.history-table {
  border-collapse: collapse;
  font-family: var(--font-body);
  font-size: 0.82rem;
  min-width: 480px;
}

.history-table thead tr {
  border-bottom: 1px solid var(--c-border);
}

.history-table th {
  padding: 8px 10px;
  font-size: 0.7rem;
  letter-spacing: 0.07em;
  text-transform: uppercase;
  color: var(--c-muted);
  font-weight: 400;
  text-align: center;
}

.hcol-chain {
  text-align: left !important;
  min-width: 140px;
}

.hcol-week { width: 72px; }

.history-row {
  border-bottom: 1px solid rgba(244, 239, 224, 0.04);
}

.history-row:hover { background: var(--c-surface); }

.history-table td {
  padding: 10px 10px;
  vertical-align: middle;
}

.chain-label-cell {
  color: var(--c-ivory);
  font-size: 0.85rem;
}

.grade-cell {
  text-align: center;
}

.history-score {
  display: block;
  font-size: 0.68rem;
  color: var(--c-muted);
  margin-top: 2px;
}

/* ── State blocks ────────────────────────────────────── */
.state-block {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 16px;
  padding: 80px 0;
  text-align: center;
}

.state-icon {
  font-size: 3rem;
  color: var(--c-border);
}

.state-text {
  font-family: var(--font-display);
  font-size: 1.2rem;
  color: var(--c-muted);
  max-width: 440px;
}

.state-hint {
  font-family: var(--font-body);
  font-size: 0.8rem;
  color: var(--c-muted);
  margin: 0;
  opacity: 0.7;
  max-width: 440px;
  line-height: 1.6;
}

.state-hint code {
  background: rgba(255, 255, 255, 0.07);
  padding: 2px 6px;
  border-radius: 2px;
  font-size: 0.75rem;
}

.state-block.error .state-text { color: #e74c3c; }

.spinner {
  display: block;
  width: 36px;
  height: 36px;
  border: 2px solid var(--c-border);
  border-top-color: var(--c-amber);
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin { to { transform: rotate(360deg); } }

/* ── Responsive ──────────────────────────────────────── */
@media (max-width: 600px) {
  .chain-table th:is(.col-items),
  .chain-table td:is(.col-items) { display: none; }

  .chain-logo-wrap { width: 36px; }
}
</style>
