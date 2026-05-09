<script setup>
import ScoreMeter from './ScoreMeter.vue'

const props = defineProps({
  deal: { type: Object, required: true },
  index: { type: Number, default: 0 }
})

function formatPrice(v) {
  if (v == null) return '—'
  return `$${parseFloat(v).toFixed(2)}`
}

function formatDate(d) {
  if (!d) return ''
  try {
    return new Date(d).toLocaleDateString('en-CA', { month: 'short', day: 'numeric' })
  } catch { return d }
}

function discountPct(deal) {
  const s = parseFloat(deal.sale_price)
  const r = parseFloat(deal.regular_price)
  if (!r || r <= s) return null
  return Math.round((1 - s / r) * 100)
}

function promoLabel(type) {
  const MAP = {
    percentage_off: '% Off', dollar_off: '$ Off', multi_buy: 'Multi-Buy',
    bogo: 'BOGO', loyalty_points: 'Points', member_price: 'Member',
    clearance: 'Clearance', rollback: 'Rollback', no_promo: null
  }
  return MAP[type] ?? null
}
</script>

<template>
  <article
    class="deal-card"
    :style="{ animationDelay: `${index * 0.06}s` }"
  >
    <div class="card-left">
      <ScoreMeter :score="deal.deal_score ?? 0" :size="68" />
    </div>

    <div class="card-body">
      <div class="card-meta">
        <span class="chain-tag">{{ deal.store_chain?.replace(/_/g, ' ') }}</span>
        <span v-if="deal.category_l1" class="cat-tag">{{ deal.category_l1 }}</span>
        <span v-if="promoLabel(deal.promo_type)" class="promo-tag">
          {{ promoLabel(deal.promo_type) }}
        </span>
      </div>

      <h3 class="product-name">{{ deal.name_en || deal.name_fr || 'Unknown Product' }}</h3>

      <p v-if="deal.brand" class="product-brand">{{ deal.brand }}</p>

      <div class="price-row">
        <span class="sale-price">{{ formatPrice(deal.sale_price) }}</span>
        <span v-if="deal.regular_price && deal.regular_price > deal.sale_price" class="reg-price">
          {{ formatPrice(deal.regular_price) }}
        </span>
        <span v-if="discountPct(deal)" class="discount-badge">
          −{{ discountPct(deal) }}%
        </span>
      </div>
    </div>

    <div class="card-right">
      <div v-if="deal.flyer_valid_to" class="validity">
        <span class="valid-label">Until</span>
        <span class="valid-date">{{ formatDate(deal.flyer_valid_to) }}</span>
      </div>
      <div v-if="deal.confidence_label" class="confidence-badge" :class="deal.confidence_label.toLowerCase()">
        {{ deal.confidence_label }}
      </div>
    </div>
  </article>
</template>

<style scoped>
.deal-card {
  display: flex;
  align-items: center;
  gap: var(--space-md);
  background: var(--c-surface);
  border: 1px solid var(--c-border);
  border-radius: 4px;
  padding: var(--space-md) var(--space-lg);
  animation: card-in 0.5s both ease-out;
  transition: transform 0.2s, border-color 0.2s, box-shadow 0.2s;
  cursor: default;
}

.deal-card:hover {
  transform: translateY(-2px);
  border-color: rgba(240, 165, 0, 0.35);
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
}

@keyframes card-in {
  from {
    opacity: 0;
    transform: translateY(16px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.card-left {
  flex-shrink: 0;
}

.card-body {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 6px;
  min-width: 0;
}

.card-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  align-items: center;
}

.chain-tag {
  font-family: var(--font-body);
  font-size: 0.65rem;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--c-ivory);
  background: rgba(255,255,255,0.07);
  border-radius: 2px;
  padding: 2px 7px;
}

.cat-tag {
  font-family: var(--font-body);
  font-size: 0.65rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--c-muted);
}

.promo-tag {
  font-family: var(--font-display);
  font-size: 0.65rem;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--c-amber);
  border: 1px solid var(--c-amber);
  border-radius: 2px;
  padding: 1px 6px;
}

.product-name {
  font-family: var(--font-display);
  font-size: 1.05rem;
  font-weight: 600;
  color: var(--c-ivory);
  margin: 0;
  line-height: 1.3;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.product-brand {
  font-family: var(--font-body);
  font-size: 0.75rem;
  color: var(--c-muted);
  font-style: italic;
  margin: 0;
}

.price-row {
  display: flex;
  align-items: baseline;
  gap: 10px;
}

.sale-price {
  font-family: var(--font-display);
  font-size: 1.4rem;
  font-weight: 700;
  color: var(--c-ivory);
}

.reg-price {
  font-family: var(--font-body);
  font-size: 0.85rem;
  color: var(--c-muted);
  text-decoration: line-through;
}

.discount-badge {
  font-family: var(--font-display);
  font-size: 0.75rem;
  font-weight: 700;
  color: #e74c3c;
  background: rgba(231, 76, 60, 0.1);
  border: 1px solid rgba(231, 76, 60, 0.3);
  border-radius: 2px;
  padding: 1px 6px;
  letter-spacing: 0.04em;
}

.card-right {
  flex-shrink: 0;
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 8px;
}

.validity {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
}

.valid-label {
  font-family: var(--font-body);
  font-size: 0.6rem;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: var(--c-muted);
}

.valid-date {
  font-family: var(--font-display);
  font-size: 0.9rem;
  font-weight: 600;
  color: var(--c-ivory);
}

.confidence-badge {
  font-family: var(--font-body);
  font-size: 0.6rem;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  border-radius: 2px;
  padding: 2px 7px;
  border: 1px solid;
}

.confidence-badge.high {
  color: #27ae60;
  border-color: rgba(39, 174, 96, 0.4);
  background: rgba(39, 174, 96, 0.08);
}

.confidence-badge.medium {
  color: #f39c12;
  border-color: rgba(243, 156, 18, 0.4);
  background: rgba(243, 156, 18, 0.08);
}

.confidence-badge.low {
  color: var(--c-muted);
  border-color: var(--c-border);
}
</style>
