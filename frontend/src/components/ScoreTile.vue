<script setup>
defineProps({
  value: { type: Number, default: null },
})
</script>

<template>
  <div class="score-tile">
    <span class="tile-value">{{ value != null ? Math.round(value) : '—' }}</span>
    <div class="tile-bar-track">
      <div
        class="tile-bar-fill"
        :style="{ width: value != null ? `${Math.min(100, value)}%` : '0%' }"
      />
    </div>
  </div>
</template>

<style scoped>
.score-tile {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 4px;
  opacity: 0.3;
  transition: opacity 0.2s;
}

.tile-value {
  font-family: var(--font-display);
  font-size: 1.05rem;
  font-weight: 600;
  color: var(--c-ivory);
  line-height: 1;
}

.tile-bar-track {
  width: 80%;
  height: 3px;
  background: rgba(244, 239, 224, 0.1);
  border-radius: 2px;
  overflow: hidden;
}

.tile-bar-fill {
  height: 100%;
  background: var(--c-amber);
  border-radius: 2px;
  transition: width 0.5s ease;
}


</style>
