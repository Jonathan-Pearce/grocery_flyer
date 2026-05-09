<script setup>
/**
 * Circular SVG score arc — fills amber→red based on 0–100 score.
 */
const props = defineProps({
  score: { type: Number, default: 0 },
  size: { type: Number, default: 64 }
})

const R = 26
const CIRC = 2 * Math.PI * R
const dash = (v) => (v / 100) * CIRC

function scoreColor(score) {
  if (score >= 80) return '#e74c3c'
  if (score >= 60) return '#e67e22'
  return '#f0a500'
}
</script>

<template>
  <div class="score-meter" :style="{ width: `${size}px`, height: `${size}px` }">
    <svg :width="size" :height="size" viewBox="0 0 60 60">
      <!-- Track -->
      <circle
        cx="30" cy="30" :r="R"
        fill="none"
        stroke="rgba(255,255,255,0.06)"
        stroke-width="5"
        stroke-linecap="round"
      />
      <!-- Arc -->
      <circle
        cx="30" cy="30" :r="R"
        fill="none"
        :stroke="scoreColor(score)"
        stroke-width="5"
        stroke-linecap="round"
        :stroke-dasharray="`${dash(score)} ${CIRC}`"
        stroke-dashoffset="0"
        transform="rotate(-90 30 30)"
        class="arc"
      />
      <!-- Score number -->
      <text
        x="30" y="30"
        text-anchor="middle"
        dominant-baseline="central"
        class="score-num"
        :fill="scoreColor(score)"
      >{{ score }}</text>
    </svg>
    <div class="score-label">Deal Score</div>
  </div>
</template>

<style scoped>
.score-meter {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 2px;
  flex-shrink: 0;
}

.arc {
  transition: stroke-dasharray 0.6s cubic-bezier(0.4, 0, 0.2, 1);
}

.score-num {
  font-family: var(--font-display);
  font-size: 16px;
  font-weight: 700;
}

.score-label {
  font-family: var(--font-body);
  font-size: 0.55rem;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--c-muted);
  line-height: 1;
}
</style>
