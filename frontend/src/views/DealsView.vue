<script setup>
import { onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { useUserStore } from '@/stores/user.js'
import { useDealsStore } from '@/stores/deals.js'
import DealCard from '@/components/DealCard.vue'
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
        <button class="reset-btn" @click="deals.setCategory(null)">Clear filter</button>
      </div>

      <!-- Deal cards -->
      <div v-else class="deals-grid">
        <DealCard
          v-for="(deal, i) in deals.filteredDeals"
          :key="`${deal.store_chain}-${deal.sku}-${i}`"
          :deal="deal"
          :index="i"
        />
      </div>
    </div>
  </main>
</template>

<style scoped>
.deals-view {
  flex: 1;
  background: var(--c-bg);
}

.deals-inner {
  max-width: 900px;
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
  align-items: baseline;
  justify-content: space-between;
  gap: var(--space-md);
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

.deals-grid {
  display: flex;
  flex-direction: column;
  gap: 10px;
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
