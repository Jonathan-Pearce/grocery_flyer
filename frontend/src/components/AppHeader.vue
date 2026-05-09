<script setup>
import { useRouter, useRoute } from 'vue-router'
import { useUserStore } from '@/stores/user.js'
import { computed } from 'vue'

const router = useRouter()
const route = useRoute()
const user = useUserStore()

const onDealsPage = computed(() => route.name === 'deals')

function goHome() {
  router.push('/')
}
</script>

<template>
  <header class="app-header">
    <div class="header-inner">
      <button class="logo" @click="goHome" aria-label="Go to home">
        <span class="logo-mark">◈</span>
        <span class="logo-text">Flyer<em>Deals</em></span>
      </button>

      <div v-if="onDealsPage && user.hasChains" class="header-chains">
        <span
          v-for="chainId in [...user.selectedChains]"
          :key="chainId"
          class="chain-chip"
        >{{ chainId.replace(/_/g, ' ') }}</span>
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
  max-width: 1200px;
  margin: 0 auto;
  padding: 0 var(--space-lg);
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

.header-chains {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  flex: 1;
}

.chain-chip {
  font-family: var(--font-body);
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--c-ivory);
  border: 1px solid var(--c-border);
  border-radius: 2px;
  padding: 2px 8px;
  opacity: 0.7;
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
