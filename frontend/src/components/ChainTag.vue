<script setup>
import { ref, computed } from 'vue'
import { chainLabel, chainLogoUrl, DARK_BG_LOGOS } from '@/utils/chainLogos.js'

const props = defineProps({
  chain: { type: String, required: true },
})

const logoFailed = ref(false)
const logoPath = chainLogoUrl(props.chain)
const logoSrc = computed(() =>
  logoPath ? `${import.meta.env.BASE_URL}${logoPath}` : null
)
const label = chainLabel(props.chain)
const hasLogo = computed(() => !!logoSrc.value && !logoFailed.value)
const darkBg = DARK_BG_LOGOS.has(props.chain)
</script>

<template>
  <span class="chain-tag" :class="{ 'has-logo': hasLogo, 'dark-bg': darkBg }">
    <img
      v-if="hasLogo"
      :src="logoSrc"
      :alt="label"
      class="chain-logo"
      :data-chain="chain"
      @error="logoFailed = true"
    />
    <span v-else class="chain-text">{{ label }}</span>
  </span>
</template>

<style scoped>
/* Text pill — matches the header chain-chip style */
.chain-tag {
  display: inline-flex;
  align-items: center;
  font-family: var(--font-body);
  font-size: 0.7rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--c-ivory);
  border: 1px solid var(--c-border);
  border-radius: 2px;
  padding: 2px 8px;
  opacity: 0.7;
  vertical-align: middle;
  line-height: 1;
}

/* Logo badge — white pill so the full-colour logo reads cleanly on the dark surface */
.chain-tag.has-logo {
  background: #fff;
  border-color: rgba(255, 255, 255, 0.15);
  padding: 3px 8px;
  border-radius: 3px;
  opacity: 1;
}

/* Dark pill variant for logos with transparent + light-coloured content (e.g. Food Basics yellow) */
.chain-tag.has-logo.dark-bg {
  background: #1b2d1e;
  border-color: rgba(255, 255, 255, 0.15);
}

.chain-logo {
  display: block;
  height: 14px;
  width: auto;
  object-fit: contain;
}
</style>
