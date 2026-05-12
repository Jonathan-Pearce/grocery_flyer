<script setup>
import { computed } from 'vue'
import { categoryIcon } from '@/utils/categoryIcons.js'

const props = defineProps({
  deal: { type: Object, required: true },
  index: { type: Number, default: 0 }
})

const scoreTier = computed(() => {
  const s = props.deal.deal_score ?? 0
  if (s >= 80) return 'hot'
  if (s >= 65) return 'good'
  return 'fair'
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
    :class="`score-tier-${scoreTier}`"
    :style="{ animationDelay: `${index * 0.06}s` }"
  >
    <div class="card-body">
      <div class="card-meta">
        <span class="chain-tag">{{ deal.store_chain?.replace(/_/g, ' ') }}</span>
        <span class="meta-sep" aria-hidden="true">·</span>
        <span v-if="deal.category_l1" class="cat-tag">
          <span v-if="categoryIcon(deal.category_l1)" class="cat-icon" aria-hidden="true">{{ categoryIcon(deal.category_l1) }}</span>{{ deal.category_l1 }}
        </span>
        <span v-if="promoLabel(deal.promo_type)" class="meta-sep" aria-hidden="true">·</span>
        <span v-if="promoLabel(deal.promo_type)" class="promo-tag">{{ promoLabel(deal.promo_type) }}</span>
      </div>

      <h3 class="product-name">{{ deal.name_en || deal.name_fr || 'Unknown Product' }}</h3>

      <div class="price-row">
        <span class="sale-price">{{ formatPrice(deal.sale_price) }}</span>
        <span v-if="deal.regular_price && deal.regular_price > deal.sale_price" class="reg-price">
          {{ formatPrice(deal.regular_price) }}
        </span>
        <span v-if="discountPct(deal)" class="discount-badge">
          −{{ discountPct(deal) }}%
        </span>
        <span v-if="deal.price_unit && deal.price_unit !== 'ea'" class="price-unit">
          / {{ deal.price_unit }}
        </span>
        <span v-if="deal.brand" class="product-brand">{{ deal.brand }}</span>
      </div>
    </div>

    <div class="card-right">
      <div v-if="deal.confidence_label" class="confidence-badge" :class="deal.confidence_label.toLowerCase()">
        {{ deal.confidence_label }}
      </div>
      <div v-if="deal.flyer_valid_to" class="validity">
        <span class="valid-label">Until</span>
        <span class="valid-date">{{ formatDate(deal.flyer_valid_to) }}</span>
      </div>
    </div>
  </article>
</template>

<style scoped>
/* ── Card shell ─────────────────────────────────────────────── */
.deal-card {
  display: flex;
  align-items: center;
  gap: var(--space-md);
  background: var(--c-surface);
  border: 1px solid var(--c-border);
  border-left: 3px solid transparent;
  border-radius: 4px;
  padding: 18px var(--space-lg);
  animation: card-in 0.5s both ease-out;
  transition: transform 0.2s, border-color 0.2s, box-shadow 0.2s;
  cursor: default;
}

.deal-card:hover {
  transform: translateY(-2px);
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.45);
}

/* Left accent stripe by score tier */
.deal-card.score-tier-hot  { border-left-color: #e74c3c; }
.deal-card.score-tier-good { border-left-color: var(--c-amber); }
.deal-card.score-tier-fair { border-left-color: rgba(244, 239, 224, 0.25); }

.deal-card.score-tier-hot:hover  { border-color: rgba(231, 76, 60, 0.45); border-left-color: #e74c3c; }
.deal-card.score-tier-good:hover { border-color: rgba(240, 165, 0, 0.4); border-left-color: var(--c-amber); }
.deal-card.score-tier-fair:hover { border-color: rgba(244, 239, 224, 0.2); }

@keyframes card-in {
  from { opacity: 0; transform: translateY(14px); }
  to   { opacity: 1; transform: translateY(0); }
}

/* ── Body ───────────────────────────────────────────────────── */
.card-body {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 5px;
  min-width: 0;
}

/* Meta row ---------------------------------------------------- */
.card-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  align-items: center;
}

.chain-tag {
  font-family: var(--font-body);
  font-size: 0.7rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--c-ivory);
  border: 1px solid var(--c-border);
  border-radius: 2px;
  padding: 2px 8px;
  opacity: 0.7;
}

.meta-sep {
  color: var(--c-muted);
  font-size: 0.75rem;
  line-height: 1;
  user-select: none;
}

.cat-tag {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-family: var(--font-body);
  font-size: 0.72rem;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: rgba(244, 239, 224, 0.65);
}

.cat-icon {
  font-style: normal;
  font-size: 0.85em;
  letter-spacing: 0;
}

.promo-tag {
  font-family: var(--font-body);
  font-size: 0.72rem;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: rgba(244, 239, 224, 0.65);
}

/* Product name ------------------------------------------------ */
.product-name {
  font-family: var(--font-display);
  font-size: 1.15rem;
  font-weight: 600;
  color: var(--c-ivory);
  margin: 0;
  line-height: 1.35;
  /* Allow up to 2 lines before truncating */
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

/* Price row --------------------------------------------------- */
.price-row {
  display: flex;
  align-items: baseline;
  gap: 10px;
  flex-wrap: wrap;
}

.sale-price {
  font-family: var(--font-display);
  font-size: 1.5rem;
  font-weight: 700;
  color: var(--c-ivory);
  line-height: 1;
}

.reg-price {
  font-family: var(--font-body);
  font-size: 0.9rem;
  color: rgba(244, 239, 224, 0.4);
  text-decoration: line-through;
}

.discount-badge {
  font-family: var(--font-body);
  font-size: 0.78rem;
  font-weight: 600;
  color: #e05c4b;
  background: rgba(224, 92, 75, 0.12);
  border: 1px solid rgba(224, 92, 75, 0.35);
  border-radius: 2px;
  padding: 1px 7px;
  letter-spacing: 0.03em;
}

.price-unit {
  font-family: var(--font-body);
  font-size: 0.78rem;
  color: rgba(244, 239, 224, 0.45);
}

.product-brand {
  font-family: var(--font-body);
  font-size: 0.78rem;
  color: rgba(244, 239, 224, 0.45);
  font-style: italic;
}

/* ── Right ──────────────────────────────────────────────────── */
.card-right {
  flex-shrink: 0;
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 10px;
  min-width: 72px;
}

.confidence-badge {
  font-family: var(--font-body);
  font-size: 0.67rem;
  font-weight: 600;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  border-radius: 2px;
  padding: 3px 9px;
  border: 1px solid;
}

.confidence-badge.high {
  color: #3fc87a;
  border-color: rgba(63, 200, 122, 0.45);
  background: rgba(63, 200, 122, 0.09);
}

.confidence-badge.medium {
  color: #f0a500;
  border-color: rgba(240, 165, 0, 0.4);
  background: rgba(240, 165, 0, 0.08);
}

.confidence-badge.low {
  color: rgba(244, 239, 224, 0.4);
  border-color: rgba(244, 239, 224, 0.15);
}

.validity {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 1px;
}

.valid-label {
  font-family: var(--font-body);
  font-size: 0.63rem;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  color: rgba(244, 239, 224, 0.4);
}

.valid-date {
  font-family: var(--font-display);
  font-size: 1rem;
  font-weight: 600;
  color: var(--c-ivory);
}
</style>
