<script setup>
import { useRouter, useRoute } from 'vue-router'
import { computed } from 'vue'

const router = useRouter()
const route = useRoute()

const onHomePage = computed(() => route.name === 'home')
const onRankingsPage = computed(() => route.name === 'rankings')

function goHome() {
  router.push('/')
}

function goDeals() {
  router.push('/deals')
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
          :class="{ active: onHomePage }"
          @click="goHome"
        >
          Find Stores
        </button>
        <button
          class="nav-btn"
          :class="{ active: route.name === 'deals' }"
          @click="goDeals"
        >
          Deals
        </button>
        <button
          class="nav-btn"
          :class="{ active: onRankingsPage }"
          @click="goRankings"
        >
          Rankings
        </button>
      </nav>


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
</style>
