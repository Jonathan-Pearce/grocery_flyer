<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useUserStore } from '@/stores/user.js'
import { useStores } from '@/composables/useStores.js'
import { chainLogoUrl, DARK_BG_LOGOS } from '@/utils/chainLogos.js'
import { formatStoreName } from '@/utils/storeUtils.js'

const BASE_URL = import.meta.env.BASE_URL

const emit = defineEmits(['ready', 'radius-change'])

const user = useUserStore()
const { nearbyStores } = useStores()

// ── State ─────────────────────────────────────────────────────────────────────
const activeTab = ref('radius')   // 'radius' | 'pick'
const radiusKm  = ref(5)
const allNearby = ref([])         // stores within 75 km, sorted by distance
const loading   = ref(false)
const showAll   = ref(false)      // expand "Show more" in Pick tab

const DISPLAY_LIMIT = 15
const MAX_RADIUS    = 25

// ── Chain display names ────────────────────────────────────────────────────────
const CHAIN_LABELS = {
  adonis:                  'Adonis',
  atlantic_superstore:     'Atlantic Superstore',
  dominion:                'Dominion',
  farm_boy:                'Farm Boy',
  food_basics:             'Food Basics',
  foodland:                'Foodland',
  fortinos:                'Fortinos',
  freshco:                 'FreshCo',
  freshmart:               'Freshmart',
  iga:                     'IGA',
  independent_city_market: 'City Market',
  independent_grocer:      'Independent Grocer',
  loblaws:                 'Loblaws',
  longos:                  "Longo's",
  maxi:                    'Maxi',
  metro:                   'Metro',
  metro_qc:                'Metro QC',
  nofrills:                'No Frills',
  provigo:                 'Provigo',
  real_canadian_superstore:'Real Canadian',
  safeway:                 'Safeway',
  sobeys:                  'Sobeys',
  super_c:                 'Super C',
  walmart:                 'Walmart',
  zehrs:                   'Zehrs',
}

// ── Computed ──────────────────────────────────────────────────────────────────
const withinRadius = computed(() =>
  allNearby.value.filter(s => s.distanceKm <= radiusKm.value)
)

const pickListFull = computed(() => allNearby.value)
const pickListVisible = computed(() =>
  showAll.value ? pickListFull.value : pickListFull.value.slice(0, DISPLAY_LIMIT)
)
const hiddenCount = computed(() => Math.max(0, pickListFull.value.length - DISPLAY_LIMIT))

const selectedCount = computed(() => user.selectedStoreCodes.size)

function storeKey(s) { return `${s.chain}:${s.store_code}` }
function isSelected(s) { return user.selectedStoreCodes.has(storeKey(s)) }

// ── Load stores ───────────────────────────────────────────────────────────────
async function load() {
  if (!user.latlng) return
  loading.value = true
  try {
    allNearby.value = (await nearbyStores(user.latlng.lat, user.latlng.lng, MAX_RADIUS))
      .filter(s => !(s.chain === 'walmart' && /penguin\s*pick\s*up/i.test(s.store_name)))
  } finally {
    loading.value = false
  }
}

onMounted(load)
watch(() => user.latlng, load)

// ── Radius tab actions ─────────────────────────────────────────────────────────
watch(radiusKm, km => emit('radius-change', km))

// Auto-select all stores within the radius whenever it changes
watch(withinRadius, (stores) => {
  user.setStoreCodesFromRadius(stores)
}, { immediate: true })

// ── Pick tab actions ───────────────────────────────────────────────────────────
function toggleStore(s) {
  user.toggleStoreCode(s.chain, s.store_code)
}

function selectAll() {
  user.setStoreCodesFromRadius(allNearby.value)
}

/** Count stores per chain from an array of store objects */
function chainCounts(stores) {
  const counts = {}
  for (const s of stores) counts[s.chain] = (counts[s.chain] ?? 0) + 1
  return counts
}


</script>

<template>
  <div class="store-selector">
    <!-- Header -->
    <div class="ss-header">
      <h3 class="ss-title">Select Your Stores</h3>
      <p class="ss-sub" v-if="!user.latlng">Enter a postal code first</p>
      <p class="ss-sub" v-else>{{ allNearby.length }} stores within {{ MAX_RADIUS }} km</p>
    </div>

    <!-- Tabs -->
    <div class="ss-tabs" role="tablist">
      <button
        class="ss-tab"
        :class="{ active: activeTab === 'radius' }"
        role="tab"
        :aria-selected="activeTab === 'radius'"
        @click="activeTab = 'radius'"
      >By Radius</button>
      <button
        class="ss-tab"
        :class="{ active: activeTab === 'pick' }"
        role="tab"
        :aria-selected="activeTab === 'pick'"
        @click="activeTab = 'pick'"
      >Pick Stores</button>
    </div>

    <!-- Loading -->
    <div v-if="loading" class="ss-loading">
      <span class="mini-spinner" />
      <span>Finding nearby stores…</span>
    </div>

    <!-- ── Tab A: By Radius ──────────────────────────────────────────────── -->
    <div v-else-if="activeTab === 'radius'" class="tab-panel" role="tabpanel">
      <div class="radius-row">
        <span class="radius-label">Radius</span>
        <input
          type="range"
          class="radius-slider"
          :min="0.5"
          :max="MAX_RADIUS"
          :step="0.5"
          v-model.number="radiusKm"
          aria-label="Search radius in km"
        />
        <span class="radius-value">{{ radiusKm }} km</span>
      </div>

      <p class="radius-count">
        <strong>{{ withinRadius.length }}</strong>
        store{{ withinRadius.length !== 1 ? 's' : '' }} within {{ radiusKm }} km
      </p>

      <!-- Chain summary chips -->
      <div v-if="withinRadius.length" class="chain-summary">
        <span
          v-for="(count, chain) in chainCounts(withinRadius)"
          :key="chain"
          class="chain-chip"
          :class="{ 'has-logo': !!chainLogoUrl(chain), 'dark-bg': DARK_BG_LOGOS.has(chain) }"
        >
          <img
            v-if="chainLogoUrl(chain)"
            :src="`${BASE_URL}${chainLogoUrl(chain)}`"
            :alt="CHAIN_LABELS[chain] ?? chain"
            class="chip-logo"
          />
          <template v-else>{{ CHAIN_LABELS[chain] ?? chain }}</template>
          <span class="chip-count">({{ count }})</span>
        </span>
      </div>
      <p v-else class="ss-empty">No stores found — try a larger radius.</p>
    </div>

    <!-- ── Tab B: Pick Stores ────────────────────────────────────────────── -->
    <div v-else class="tab-panel" role="tabpanel">
      <div v-if="allNearby.length === 0" class="ss-empty">No stores within {{ MAX_RADIUS }} km.</div>
      <template v-else>
        <div class="pick-toolbar">
          <button class="pick-util-btn" @click="selectAll">Select all</button>
          <button class="pick-util-btn" @click="user.clearStoreCodes()">Clear</button>
        </div>

        <ul class="store-list">
          <li
            v-for="s in pickListVisible"
            :key="storeKey(s)"
            class="store-row"
            :class="{ selected: isSelected(s) }"
            @click="toggleStore(s)"
          >
            <span class="store-check" :aria-hidden="true">
              <svg v-if="isSelected(s)" width="12" height="12" viewBox="0 0 12 12" fill="none">
                <path d="M2 6l3 3 5-5" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
              </svg>
            </span>
            <span
              class="store-chain-badge"
              :class="{ 'has-logo': !!chainLogoUrl(s.chain), 'dark-bg': DARK_BG_LOGOS.has(s.chain) }"
            >
              <img
                v-if="chainLogoUrl(s.chain)"
                :src="`${BASE_URL}${chainLogoUrl(s.chain)}`"
                :alt="CHAIN_LABELS[s.chain] ?? s.chain"
                class="badge-logo"
              />
              <template v-else>{{ CHAIN_LABELS[s.chain] ?? s.chain }}</template>
            </span>
            <span class="store-body">
              <span class="store-name">{{ formatStoreName(s.chain, s.store_name) }}</span>
              <span class="store-meta">
                <span v-if="s.city" class="store-city">{{ s.city }}</span>
                <span class="store-dist">{{ s.distanceKm }} km</span>
              </span>
            </span>
          </li>
        </ul>

        <button
          v-if="hiddenCount > 0 && !showAll"
          class="show-more-btn"
          @click="showAll = true"
        >
          Show {{ hiddenCount }} more store{{ hiddenCount !== 1 ? 's' : '' }}
        </button>
      </template>
    </div>

    <!-- Footer CTA -->
    <div class="ss-footer">
      <span class="ss-count">
        {{ selectedCount }} store{{ selectedCount !== 1 ? 's' : '' }} selected
      </span>
      <button
        class="ss-cta"
        :disabled="selectedCount === 0"
        @click="emit('ready')"
      >
        Show Deals →
      </button>
    </div>
  </div>
</template>

<style scoped>
.store-selector {
  background: var(--c-surface);
  border: 1px solid var(--c-border);
  border-radius: 4px;
  display: flex;
  flex-direction: column;
  gap: var(--space-md);
  padding: var(--space-lg);
}

/* Header */
.ss-header { display: flex; flex-direction: column; gap: 4px; }
.ss-title {
  font-family: var(--font-display);
  font-size: 1.3rem;
  font-weight: 600;
  color: var(--c-ivory);
  margin: 0;
}
.ss-sub {
  font-family: var(--font-body);
  font-size: 0.75rem;
  color: var(--c-muted);
  letter-spacing: 0.05em;
  margin: 0;
  text-transform: uppercase;
}

/* Tabs */
.ss-tabs {
  display: flex;
  border-bottom: 1px solid var(--c-border);
  gap: 0;
}
.ss-tab {
  flex: 1;
  background: none;
  border: none;
  border-bottom: 2px solid transparent;
  color: var(--c-muted);
  font-family: var(--font-body);
  font-size: 0.8rem;
  letter-spacing: 0.05em;
  padding: 8px 0;
  cursor: pointer;
  transition: color 0.15s, border-color 0.15s;
  text-transform: uppercase;
}
.ss-tab:hover { color: var(--c-ivory); }
.ss-tab.active {
  color: var(--c-amber);
  border-bottom-color: var(--c-amber);
}

/* Loading */
.ss-loading {
  display: flex;
  align-items: center;
  gap: 8px;
  color: var(--c-muted);
  font-family: var(--font-body);
  font-size: 0.82rem;
  padding: var(--space-md) 0;
}
.mini-spinner {
  display: inline-block;
  width: 14px;
  height: 14px;
  border: 2px solid var(--c-border);
  border-top-color: var(--c-amber);
  border-radius: 50%;
  animation: spin 0.7s linear infinite;
  flex-shrink: 0;
}
@keyframes spin { to { transform: rotate(360deg); } }

/* Tab panel */
.tab-panel { display: flex; flex-direction: column; gap: var(--space-sm); }

/* Radius tab */
.radius-row {
  display: flex;
  align-items: center;
  gap: var(--space-sm);
}
.radius-label {
  font-family: var(--font-body);
  font-size: 0.78rem;
  color: var(--c-muted);
  letter-spacing: 0.05em;
  text-transform: uppercase;
  width: 44px;
  flex-shrink: 0;
}
.radius-slider {
  flex: 1;
  accent-color: var(--c-amber);
  cursor: pointer;
}
.radius-value {
  font-family: var(--font-display);
  font-size: 0.9rem;
  color: var(--c-amber);
  width: 44px;
  text-align: right;
  flex-shrink: 0;
}
.radius-count {
  font-family: var(--font-body);
  font-size: 0.82rem;
  color: var(--c-muted);
  margin: 0;
}
.radius-count strong { color: var(--c-ivory); }

.chain-summary {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}
.chain-chip {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-family: var(--font-body);
  font-size: 0.72rem;
  padding: 3px 8px;
  border-radius: 2px;
  background: var(--c-bg);
  border: 1px solid var(--c-border);
  color: var(--c-muted);
}
.chain-chip.has-logo {
  background: #fff;
  border-color: rgba(255,255,255,0.15);
  padding: 3px 6px;
}
.chain-chip.has-logo.dark-bg {
  background: #1b2d1e;
}
.chip-logo {
  display: block;
  height: 12px;
  width: auto;
  object-fit: contain;
}
.chip-count {
  color: #111;
  font-size: 0.7rem;
}
.chain-chip.dark-bg .chip-count {
  color: #fff;
}

/* Pick tab */
.pick-toolbar {
  display: flex;
  gap: 8px;
}
.pick-util-btn {
  background: none;
  border: 1px solid var(--c-border);
  border-radius: 2px;
  color: var(--c-muted);
  cursor: pointer;
  font-family: var(--font-body);
  font-size: 0.75rem;
  letter-spacing: 0.04em;
  padding: 4px 10px;
  transition: border-color 0.15s, color 0.15s;
}
.pick-util-btn:hover { border-color: var(--c-amber); color: var(--c-ivory); }

.store-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 2px;
  max-height: 320px;
  overflow-y: auto;
}
.store-row {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 7px 8px;
  border-radius: 2px;
  cursor: pointer;
  border: 1px solid transparent;
  transition: background 0.1s, border-color 0.1s;
}
.store-row:hover { background: var(--c-bg); }
.store-row.selected {
  background: color-mix(in srgb, var(--c-amber) 10%, transparent);
  border-color: color-mix(in srgb, var(--c-amber) 30%, transparent);
}

.store-check {
  width: 18px;
  height: 18px;
  flex-shrink: 0;
  border: 1px solid var(--c-border);
  border-radius: 2px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--c-amber);
  background: var(--c-bg);
}
.store-row.selected .store-check {
  background: var(--c-amber);
  border-color: var(--c-amber);
  color: var(--c-bg);
}

.store-chain-badge {
  display: inline-flex;
  align-items: center;
  font-family: var(--font-body);
  font-size: 0.65rem;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  padding: 2px 5px;
  border-radius: 2px;
  background: var(--c-bg);
  border: 1px solid var(--c-border);
  color: var(--c-muted);
  flex-shrink: 0;
  white-space: nowrap;
  margin-top: 1px;
}
.store-chain-badge.has-logo {
  background: #fff;
  border-color: rgba(255,255,255,0.15);
  padding: 2px 4px;
}
.store-chain-badge.has-logo.dark-bg {
  background: #1b2d1e;
}
.badge-logo {
  display: block;
  height: 12px;
  width: auto;
  object-fit: contain;
}
.store-body {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 2px;
}
.store-name {
  font-family: var(--font-body);
  font-size: 0.82rem;
  color: var(--c-ivory);
  line-height: 1.3;
}
.store-meta {
  display: flex;
  gap: 8px;
  align-items: baseline;
}
.store-city {
  font-family: var(--font-body);
  font-size: 0.72rem;
  color: var(--c-muted);
}
.store-dist {
  font-family: var(--font-body);
  font-size: 0.72rem;
  color: var(--c-muted);
  margin-left: auto;
}

.show-more-btn {
  background: none;
  border: 1px solid var(--c-border);
  border-radius: 2px;
  color: var(--c-muted);
  cursor: pointer;
  font-family: var(--font-body);
  font-size: 0.75rem;
  padding: 5px 10px;
  align-self: flex-start;
  transition: border-color 0.15s, color 0.15s;
}
.show-more-btn:hover { border-color: var(--c-amber); color: var(--c-ivory); }

.ss-empty {
  font-family: var(--font-body);
  font-size: 0.82rem;
  color: var(--c-muted);
  margin: 0;
  padding: var(--space-sm) 0;
}

/* Footer */
.ss-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  border-top: 1px solid var(--c-border);
  padding-top: var(--space-md);
  gap: var(--space-md);
}
.ss-count {
  font-family: var(--font-body);
  font-size: 0.78rem;
  color: var(--c-muted);
}
.ss-cta {
  background: var(--c-amber);
  border: none;
  border-radius: 2px;
  color: var(--c-bg);
  cursor: pointer;
  font-family: var(--font-body);
  font-size: 0.82rem;
  font-weight: 600;
  letter-spacing: 0.04em;
  padding: 8px 20px;
  transition: opacity 0.15s;
}
.ss-cta:disabled { opacity: 0.35; cursor: default; }
.ss-cta:not(:disabled):hover { opacity: 0.85; }
</style>
