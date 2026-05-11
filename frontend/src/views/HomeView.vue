<script setup>
import { ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { useUserStore } from '@/stores/user.js'
import PostalInput from '@/components/PostalInput.vue'
import MapView from '@/components/MapView.vue'
import StoreSelector from '@/components/StoreSelector.vue'

const router = useRouter()
const user = useUserStore()

const postalCode = ref(user.postalCode || '')
const confirmedPostal = ref(user.postalCode || '')
const step = ref(user.postalCode ? 2 : 1)
const radiusKm = ref(25)

function onPostalConfirmed(code) {
  user.setPostalCode(code)
  confirmedPostal.value = code
  step.value = 2
}

function onReady() {
  router.push('/deals')
}

function onRadiusChange(km) {
  radiusKm.value = km
}
</script>

<template>
  <main class="home-view">
    <!-- Left panel -->
    <div class="left-panel">
      <div class="panel-inner">
        <div class="hero-text">
          <h1 class="hero-heading">
            Find the best<br />
            <em>grocery deals</em><br />
            near you.
          </h1>
          <p class="hero-sub">
            Ranked by our deal score — factoring in discount depth,
            price history, and item essentiality.
          </p>
        </div>

        <!-- Step 1: Postal code -->
        <section class="step" :class="{ done: step >= 2 }">
          <div class="step-num">01</div>
          <PostalInput
            v-model="postalCode"
            @confirmed="onPostalConfirmed"
          />
        </section>

        <!-- Step 2: Store selector -->
        <Transition name="slide-up">
          <section v-if="step >= 2" class="step">
            <div class="step-num">02</div>
            <StoreSelector
              @ready="onReady"
              @radius-change="onRadiusChange"
            />
          </section>
        </Transition>
      </div>
    </div>

    <!-- Right panel: map -->
    <div class="right-panel">
      <MapView :postal-code="confirmedPostal" :radius-km="radiusKm" />
      <div class="map-overlay-label" v-if="!confirmedPostal">
        <span>Enter your postal code to see your area</span>
      </div>
    </div>
  </main>
</template>

<style scoped>
.home-view {
  flex: 1;
  display: grid;
  grid-template-columns: 480px 1fr;
  min-height: calc(100vh - 60px);
}

.left-panel {
  border-right: 1px solid var(--c-border);
  overflow-y: auto;
}

.panel-inner {
  display: flex;
  flex-direction: column;
  gap: var(--space-xl);
  padding: var(--space-xl) var(--space-xl);
}

.hero-heading {
  font-family: var(--font-display);
  font-size: clamp(2rem, 4vw, 3rem);
  font-weight: 700;
  line-height: 1.1;
  color: var(--c-ivory);
  margin: 0;
  letter-spacing: -0.01em;
}

.hero-heading em {
  font-style: italic;
  color: var(--c-amber);
}

.hero-sub {
  font-family: var(--font-body);
  font-size: 0.9rem;
  line-height: 1.6;
  color: var(--c-muted);
  margin: 12px 0 0;
}

.step {
  display: flex;
  gap: var(--space-md);
  align-items: flex-start;
}

.step-num {
  font-family: var(--font-display);
  font-size: 2.5rem;
  font-weight: 700;
  color: var(--c-border);
  line-height: 1;
  flex-shrink: 0;
  width: 44px;
  transition: color 0.3s;
  padding-top: 4px;
}

.step.done .step-num {
  color: var(--c-amber);
}

.step > :not(.step-num) {
  flex: 1;
}

/* Right panel */
.right-panel {
  position: relative;
  background: #111;
}

.right-panel > :first-child {
  position: absolute;
  inset: 0;
}

.map-overlay-label {
  position: absolute;
  bottom: var(--space-xl);
  left: 50%;
  transform: translateX(-50%);
  background: rgba(27, 45, 30, 0.85);
  border: 1px solid var(--c-border);
  color: var(--c-muted);
  font-family: var(--font-body);
  font-size: 0.8rem;
  letter-spacing: 0.06em;
  padding: 8px 20px;
  border-radius: 2px;
  pointer-events: none;
  white-space: nowrap;
}

/* Transitions */
.slide-up-enter-active {
  transition: all 0.4s cubic-bezier(0.16, 1, 0.3, 1);
}
.slide-up-enter-from {
  opacity: 0;
  transform: translateY(20px);
}

/* Responsive */
@media (max-width: 768px) {
  .home-view {
    grid-template-columns: 1fr;
    grid-template-rows: auto 300px;
  }

  .right-panel {
    height: 300px;
  }

  .right-panel > :first-child {
    height: 300px;
    position: relative;
  }
}
</style>
