<script setup>
import { useRouter, useRoute } from 'vue-router'
import { useUserStore } from '@/stores/user.js'
import { computed } from 'vue'
import ChainTag from './ChainTag.vue'

const router = useRouter()
const route = useRoute()
const user = useUserStore()

const onDealsPage = computed(() => route.name === 'deals')
const onRankingsPage = computed(() => route.name === 'rankings')

const selectedChainSlugs = computed(() => {
  const chains = new Set()
  for (const key of user.selectedStoreCodes) {
    chains.add(key.split(':')[0])
  }
  return [...chains].sort()
})

function goHome() {
  router.push('/')
}

function goRankings() {
  router.push('/rankings')
}
</script>

<template>
  <header class="app-header">
    <div class="header-inner">
      <button class="logo" @click="goHome" aria-label="Go to home">
        <span class="logo-mark">◈</span>
        <span class="logo-text">Flyer<em>Deals</em></span>
      </button>

      <nav class="header-nav">
        <button
          class="nav-btn"
          :class="{ active: onRankingsPage }"
          @click="goRankings"
        >
          Rankings
        </button>
      </nav>

      <div v-if="onDealsPage && user.hasStores" class="header-chains">
        <ChainTag
          v-for="slug in selectedChainSlugs"
          :key="slug"
          :chain="slug"
        />
        <button class="change-btn" @click="goHome">← Change Stores</button>
      </div>
    </div>
  </header>
</template>

<style scoped>
.app-header {
  position: sticky;
  top: 0;
  z-index: 200;
  background: var(--c-bg);
  border-bottom: 1px solid var(--c-border);
  backdrop-filter: blur(8px);
}

.header-inner {
  padding: 0 var(--space-xl);
  height: 60px;
  display: flex;
  align-items: center;
  gap: var(--space-lg);
}

.logo {
  display: flex;
  align-items: center;
  gap: 10px;
  background: none;
  border: none;
  cursor: pointer;
  padding: 0;
  color: var(--c-text);
  text-decoration: none;
}

.logo-mark {
  color: var(--c-amber);
  font-size: 1.4rem;
  line-height: 1;
}

.logo-text {
  font-family: var(--font-display);
  font-size: 1.4rem;
  font-weight: 600;
  letter-spacing: 0.02em;
  color: var(--c-ivory);
}

.logo-text em {
  font-style: italic;
  color: var(--c-amber);
}

.header-nav {
  display: flex;
  align-items: center;
  gap: 4px;
}

.nav-btn {
  font-family: var(--font-body);
  font-size: 0.78rem;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--c-muted);
  background: none;
  border: none;
  cursor: pointer;
  padding: 6px 10px;
  border-radius: 2px;
  transition: color 0.15s, background 0.15s;
}

.nav-btn:hover {
  color: var(--c-ivory);
  background: rgba(244, 239, 224, 0.06);
}

.nav-btn.active {
  color: var(--c-amber);
}

.header-chains {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  flex: 1;
}

.change-btn {
  margin-left: auto;
  background: none;
  border: 1px solid var(--c-amber);
  color: var(--c-amber);
  font-family: var(--font-body);
  font-size: 0.75rem;
  letter-spacing: 0.06em;
  padding: 4px 12px;
  cursor: pointer;
  border-radius: 2px;
  transition: background 0.2s, color 0.2s;
}
.change-btn:hover {
  background: var(--c-amber);
  color: var(--c-bg);
}
</style>
