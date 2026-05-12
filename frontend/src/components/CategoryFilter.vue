<script setup>
import { useDealsStore } from '@/stores/deals.js'
import { categoryIcon } from '@/utils/categoryIcons.js'

const deals = useDealsStore()
</script>

<template>
  <div class="category-filter">
    <button
      class="cat-chip"
      :class="{ active: deals.activeCategory === null }"
      @click="deals.setCategory(null)"
    >
      All
    </button>
    <button
      v-for="cat in deals.categories"
      :key="cat"
      class="cat-chip"
      :class="{ active: deals.activeCategory === cat }"
      @click="deals.setCategory(cat)"
    >
      <span v-if="categoryIcon(cat)" class="chip-icon" aria-hidden="true">{{ categoryIcon(cat) }}</span>{{ cat }}
    </button>
  </div>
</template>

<style scoped>
.category-filter {
  display: flex;
  gap: 8px;
  overflow-x: auto;
  padding: 4px 0 8px;
  scrollbar-width: none;
  -webkit-overflow-scrolling: touch;
}

.category-filter::-webkit-scrollbar {
  display: none;
}

.chip-icon {
  font-style: normal;
  margin-right: 5px;
  font-size: 0.9em;
}

.cat-chip {
  flex-shrink: 0;
  font-family: var(--font-body);
  font-size: 0.75rem;
  letter-spacing: 0.07em;
  text-transform: uppercase;
  color: var(--c-muted);
  background: none;
  border: 1px solid var(--c-border);
  border-radius: 2px;
  padding: 5px 14px;
  cursor: pointer;
  transition: color 0.15s, border-color 0.15s, background 0.15s;
  white-space: nowrap;
}

.cat-chip:hover {
  color: var(--c-ivory);
  border-color: rgba(255,255,255,0.25);
}

.cat-chip.active {
  color: var(--c-bg);
  background: var(--c-ivory);
  border-color: var(--c-ivory);
  font-weight: 500;
}
</style>
