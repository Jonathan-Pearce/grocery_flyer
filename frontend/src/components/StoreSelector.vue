<script setup>
import { ref, computed, onMounted } from 'vue'
import { useUserStore } from '@/stores/user.js'

const props = defineProps({
  province: { type: String, default: '' }
})
const emit = defineEmits(['ready'])

const user = useUserStore()
const chains = ref([])

onMounted(async () => {
  const res = await fetch('/data/chain_regions.json')
  chains.value = await res.json()
})

const availableChains = computed(() =>
  props.province
    ? chains.value.filter(c => c.provinces.includes(props.province))
    : chains.value
)

function toggle(chainId) {
  user.toggleChain(chainId)
}

function isSelected(chainId) {
  return user.selectedChains.has(chainId)
}
</script>

<template>
  <div class="store-selector">
    <div class="ss-header">
      <h3 class="ss-title">Select Your Stores</h3>
      <p class="ss-sub" v-if="province">Chains operating in {{ province }}</p>
      <p class="ss-sub" v-else>Enter a postal code to filter by region</p>
    </div>

    <div class="ss-grid">
      <button
        v-for="chain in availableChains"
        :key="chain.id"
        class="chain-btn"
        :class="{ selected: isSelected(chain.id) }"
        @click="toggle(chain.id)"
        :aria-pressed="isSelected(chain.id)"
      >
        <span class="chain-check">{{ isSelected(chain.id) ? '✓' : '' }}</span>
        <span class="chain-name">{{ chain.name }}</span>
      </button>
    </div>

    <div class="ss-footer">
      <span class="ss-count">
        {{ user.selectedChains.size }} store{{ user.selectedChains.size !== 1 ? 's' : '' }} selected
      </span>
      <button
        class="ss-cta"
        :disabled="!user.hasChains"
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

.ss-header {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

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

.ss-grid {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.chain-btn {
  display: flex;
  align-items: center;
  gap: 6px;
  background: var(--c-bg);
  border: 1px solid var(--c-border);
  color: var(--c-ivory);
  font-family: var(--font-body);
  font-size: 0.78rem;
  letter-spacing: 0.04em;
  padding: 6px 12px;
  cursor: pointer;
  border-radius: 2px;
  transition: border-color 0.15s, background 0.15s, color 0.15s;
}

.chain-btn:hover {
  border-color: var(--c-amber);
}

.chain-btn.selected {
  background: var(--c-amber);
  border-color: var(--c-amber);
  color: var(--c-bg);
  font-weight: 500;
}

.chain-check {
  font-size: 0.7rem;
  width: 10px;
  display: inline-block;
}

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
  font-size: 0.75rem;
  color: var(--c-muted);
  font-style: italic;
}

.ss-cta {
  background: var(--c-amber);
  border: none;
  color: var(--c-bg);
  font-family: var(--font-display);
  font-size: 0.9rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  padding: 10px 24px;
  cursor: pointer;
  border-radius: 2px;
  transition: background 0.2s, opacity 0.2s;
}

.ss-cta:disabled {
  opacity: 0.35;
  cursor: not-allowed;
}

.ss-cta:not(:disabled):hover {
  background: #d4900a;
}
</style>
