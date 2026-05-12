<script setup>
import { computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { useUserStore } from '@/stores/user.js'
import { useDealsStore } from '@/stores/deals.js'
import DealCard from '@/components/DealCard.vue'
import ScoreTile from '@/components/ScoreTile.vue'
import CategoryFilter from '@/components/CategoryFilter.vue'

const router = useRouter()
const user = useUserStore()
const deals = useDealsStore()

onMounted(() => {
  if (!user.hasStores) {
    router.push('/')
    return
  }
  deals.loadDeals()
})

const TIERS = [
  { key: 'hot',  label: '🔥 Hot Deals',  min: 80 },
  { key: 'good', label: '★ Good Deals',  min: 65 },
  { key: 'fair', label: 'More Deals',    min: 0  },
]

const dealGroups = computed(() => {
  const all = deals.filteredDeals
  return TIERS
    .map(tier => ({
      ...tier,
      deals: all.filter(d => {
        const s = d.deal_score ?? 0
        if (tier.key === 'hot')  return s >= 80
        if (tier.key === 'good') return s >= 65 && s < 80
        return s < 65
      }),
    }))
    .filter(g => g.deals.length > 0)
})
</script>

<template>
  <main class="deals-view">
    <div class="deals-inner">
      <!-- Sub-nav -->
      <div class="deals-header">
        <div class="deals-title-row">
          <h2 class="deals-heading">
            This Week's Best Deals
            <span v-if="deals.filteredDeals.length" class="deal-count">
              ({{ deals.filteredDeals.length }})
            </span>
          </h2>
          <div class="sort-label">Sorted by Deal Score ↓</div>
          <div class="search-wrap">
            <input
              class="search-input"
              type="search"
              placeholder="Search items…"
              :value="deals.searchQuery"
              @input="deals.setSearch($event.target.value)"
            />
          </div>
          <div class="tier-filter">
            <button
              class="tier-btn"
              :class="{ active: deals.activeTier === 'hot' }"
              @click="deals.setTier('hot')"
            >🔥 Hot</button>
            <button
              class="tier-btn"
              :class="{ active: deals.activeTier === 'good' }"
              @click="deals.setTier('good')"
            >★ Good+</button>
          </div>
        </div>
        <CategoryFilter />
      </div>

      <!-- Loading -->
      <div v-if="deals.isLoading" class="state-block">
        <span class="spinner" />
        <span class="state-text">Loading deals…</span>
      </div>

      <!-- Error -->
      <div v-else-if="deals.error" class="state-block error">
        <span class="state-icon">⚠</span>
        <span class="state-text">{{ deals.error }}</span>
        <p class="state-hint">Run <code>python scripts/export_frontend_data.py</code> to generate deal data.</p>
      </div>

      <!-- Empty (no data at all) -->
      <div v-else-if="deals.rawDeals.length === 0" class="state-block">
        <span class="state-icon">◈</span>
        <p class="state-text">No deal data available yet.</p>
        <p class="state-hint">Run the pipeline then <code>python scripts/export_frontend_data.py</code> to generate deals.</p>
      </div>

      <!-- Empty (filtered) -->
      <div v-else-if="deals.filteredDeals.length === 0" class="state-block">
        <span class="state-icon">◈</span>
        <p class="state-text">No deals found for the selected stores or category.</p>
        <div style="display:flex;gap:10px">
          <button v-if="deals.activeTier" class="reset-btn" @click="deals.setTier(null)">Clear tier filter</button>
          <button class="reset-btn" @click="deals.setCategory(null)">Clear category filter</button>
        </div>
      </div>

      <!-- Deal cards -->
      <template v-else>
        <div class="score-table">
          <!-- Sticky column-header row -->
          <div class="score-header">
            <div class="score-col-card"></div>
            <div class="score-col-label">Discount</div>
            <div class="score-col-label">Rarity</div>
            <div class="score-col-label">Essence</div>
            <div class="score-col-label">Cycle</div>
            <div class="score-col-label">Auth</div>
            <div class="score-col-label">Loyalty</div>
            <div class="score-col-label">Conf.</div>
          </div>

          <!-- Tier groups as inline dividers + deal rows -->
          <template v-for="group in dealGroups" :key="group.key">
            <div class="tier-label" :class="`tier-label-${group.key}`">{{ group.label }}</div>
            <div
              v-for="(deal, i) in group.deals"
              :key="`${deal.store_chain}-${deal.sku}-${i}`"
              class="deal-row"
            >
              <DealCard :deal="deal" :index="i" />
              <ScoreTile :value="deal.score_discount_depth != null ? deal.score_discount_depth / 25 * 100 : null" />
              <ScoreTile :value="deal.score_deal_rarity    != null ? deal.score_deal_rarity    / 20 * 100 : null" />
              <ScoreTile :value="deal.score_essentiality   != null ? deal.score_essentiality   / 20 * 100 : null" />
              <ScoreTile :value="deal.score_cycle_position != null ? deal.score_cycle_position / 15 * 100 : null" />
              <ScoreTile :value="deal.score_authenticity   != null ? deal.score_authenticity   / 15 * 100 : null" />
              <ScoreTile :value="deal.score_loyalty_bonus  != null ? deal.score_loyalty_bonus  /  5 * 100 : null" />
              <div
                class="confidence-cell"
                :class="`conf-${(deal.confidence_label || 'low').toLowerCase()}`"
              >{{ deal.confidence_label || '—' }}</div>
            </div>
          </template>
        </div>
      </template>
    </div>
  </main>
</template>

<style scoped>
.deals-view {
  flex: 1;
  background: var(--c-bg);
}

.deals-inner {
  max-width: 1200px;
  margin: 0 auto;
  padding: var(--space-xl) var(--space-lg);
  display: flex;
  flex-direction: column;
  gap: var(--space-lg);
}

.deals-header {
  display: flex;
  flex-direction: column;
  gap: var(--space-sm);
  border-bottom: 1px solid var(--c-border);
  padding-bottom: var(--space-md);
}

.deals-title-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--space-md);
  flex-wrap: wrap;
}

.deals-heading {
  font-family: var(--font-display);
  font-size: 2rem;
  font-weight: 700;
  color: var(--c-ivory);
  margin: 0;
  letter-spacing: -0.01em;
}

.deal-count {
  font-size: 1.2rem;
  color: var(--c-muted);
  font-weight: 400;
}

.sort-label {
  font-family: var(--font-body);
  font-size: 0.75rem;
  letter-spacing: 0.08em;
  color: var(--c-muted);
  text-transform: uppercase;
  white-space: nowrap;
}

.search-wrap {
  flex: 1;
  min-width: 140px;
  max-width: 260px;
}

.search-input {
  width: 100%;
  font-family: var(--font-body);
  font-size: 0.8rem;
  color: var(--c-ivory);
  background: rgba(244, 239, 224, 0.05);
  border: 1px solid var(--c-border);
  border-radius: 2px;
  padding: 5px 10px;
  outline: none;
  transition: border-color 0.15s, background 0.15s;
  box-sizing: border-box;
}

.search-input::placeholder {
  color: rgba(244, 239, 224, 0.3);
}

.search-input:focus {
  border-color: rgba(244, 239, 224, 0.35);
  background: rgba(244, 239, 224, 0.08);
}

/* Chrome/Safari removes the native ✕ button styling — keep it subtle */
.search-input::-webkit-search-cancel-button {
  opacity: 0.4;
  cursor: pointer;
}

.tier-filter {
  display: flex;
  gap: 6px;
}

.tier-btn {
  font-family: var(--font-body);
  font-size: 0.72rem;
  letter-spacing: 0.07em;
  text-transform: uppercase;
  color: var(--c-muted);
  background: none;
  border: 1px solid var(--c-border);
  border-radius: 2px;
  padding: 4px 12px;
  cursor: pointer;
  transition: color 0.15s, border-color 0.15s, background 0.15s;
  white-space: nowrap;
}

.tier-btn:hover {
  color: var(--c-ivory);
  border-color: rgba(255, 255, 255, 0.25);
}

.tier-btn.active {
  color: var(--c-ivory);
  border-color: var(--c-amber);
  background: rgba(240, 165, 0, 0.1);
}

/* ── Score table layout ─────────────────────────────────── */
.score-table {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.score-header,
.deal-row {
  display: grid;
  grid-template-columns: 1fr repeat(7, 60px);
  gap: 8px;
  align-items: center;
}

.score-header {
  position: sticky;
  top: 60px;
  z-index: 10;
  background: var(--c-bg);
  padding: 6px 0 8px;
  border-bottom: 1px solid var(--c-border);
}

.score-col-label {
  font-family: var(--font-body);
  font-size: 0.6rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--c-muted);
  text-align: center;
  line-height: 1.4;
}

/* Tier dividers within the table */
.tier-label {
  font-family: var(--font-body);
  font-size: 0.72rem;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--c-muted);
  padding: 12px 0 4px;
  border-bottom: 1px solid var(--c-border);
}

.tier-label-hot  { color: #e74c3c; border-color: rgba(231, 76, 60, 0.3); }
.tier-label-good { color: var(--c-amber); border-color: rgba(240, 165, 0, 0.3); }

/* Deal row hover — brighten score tiles */
.deal-row:hover :deep(.score-tile) {
  opacity: 1;
}

.deal-row:hover .confidence-cell {
  opacity: 1;
}

/* Confidence cell */
.confidence-cell {
  display: flex;
  align-items: center;
  justify-content: center;
  font-family: var(--font-display);
  font-size: 0.65rem;
  font-weight: 600;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  text-align: center;
  opacity: 0.3;
  transition: opacity 0.2s;
}

.conf-high   { color: #4caf7d; }
.conf-medium { color: var(--c-amber); }
.conf-low    { color: rgba(231, 76, 60, 0.8); }

@media (max-width: 720px) {
  .score-header { display: none; }
  .deal-row {
    grid-template-columns: 1fr;
  }
  .deal-row > :not(:first-child) { display: none; }
}

/* State blocks */
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
  max-width: 400px;
}

.state-hint {
  font-family: var(--font-body);
  font-size: 0.8rem;
  color: var(--c-muted);
  margin: 0;
  opacity: 0.7;
}

.state-hint code {
  background: rgba(255,255,255,0.07);
  padding: 2px 6px;
  border-radius: 2px;
  font-size: 0.75rem;
}

.state-block.error .state-text {
  color: #e74c3c;
}

.reset-btn {
  font-family: var(--font-body);
  font-size: 0.8rem;
  letter-spacing: 0.06em;
  color: var(--c-amber);
  background: none;
  border: 1px solid var(--c-amber);
  border-radius: 2px;
  padding: 6px 18px;
  cursor: pointer;
}

.reset-btn:hover {
  background: var(--c-amber);
  color: var(--c-bg);
}

/* Spinner */
.spinner {
  display: block;
  width: 36px;
  height: 36px;
  border: 2px solid var(--c-border);
  border-top-color: var(--c-amber);
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

@media (max-width: 600px) {
  .deals-title-row {
    flex-direction: column;
    gap: 4px;
  }
}
</style>
