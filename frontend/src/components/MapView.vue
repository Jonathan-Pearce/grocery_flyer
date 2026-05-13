<script setup>
import { onMounted, onUnmounted, watch, ref } from 'vue'
import { useUserStore } from '@/stores/user.js'
import { useGeocoding } from '@/composables/useGeocoding.js'
import { useStores } from '@/composables/useStores.js'
import { chainLogoUrl, chainLabel, DARK_BG_LOGOS } from '@/utils/chainLogos.js'
import { formatStoreName } from '@/utils/storeUtils.js'

const BASE_URL = import.meta.env.BASE_URL

const props = defineProps({
  postalCode: { type: String, default: '' },
  radiusKm:   { type: Number, default: 25 },
})

const user = useUserStore()
const { geocodePostalCode, geocoding } = useGeocoding()
const { nearbyStores } = useStores()

const mapEl = ref(null)
let map = null
let marker = null
let storeLayerGroup = null
let radiusCircle = null

async function initMap(coords) {
  const L = (await import('leaflet')).default

  if (!map) {
    map = L.map(mapEl.value, {
      zoomControl: true,
      attributionControl: true
    }).setView([coords.lat, coords.lng], 13)

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution: '© OpenStreetMap contributors © CARTO',
      maxZoom: 18
    }).addTo(map)

    storeLayerGroup = L.layerGroup().addTo(map)
  } else {
    map.setView([coords.lat, coords.lng], 13)
  }

  // Custom pulsing marker for user location
  const icon = L.divIcon({
    html: `<div class="map-dot"></div>`,
    className: '',
    iconSize: [24, 24],
    iconAnchor: [12, 12]
  })

  if (marker) marker.remove()
  marker = L.marker([coords.lat, coords.lng], { icon }).addTo(map)

  drawRadiusCircle(L, coords, props.radiusKm)
  await addStoreMarkers(L, coords)
}

function drawRadiusCircle(L, coords, km) {
  if (radiusCircle) radiusCircle.remove()
  radiusCircle = L.circle([coords.lat, coords.lng], {
    radius: km * 1000,
    color: '#f0a500',
    weight: 2,
    opacity: 0.85,
    fillColor: '#f0a500',
    fillOpacity: 0.08,
  }).addTo(map)
}

async function addStoreMarkers(L, coords) {
  if (!storeLayerGroup) return
  storeLayerGroup.clearLayers()

  const stores = await nearbyStores(coords.lat, coords.lng, 75)

  const fallbackIcon = L.divIcon({
    html: `<div class="store-dot"></div>`,
    className: '',
    iconSize: [12, 12],
    iconAnchor: [6, 6]
  })

  for (const s of stores) {
    const logoPath = chainLogoUrl(s.chain)
    const isDark = DARK_BG_LOGOS.has(s.chain)
    const bg = isDark ? '#1b2d1e' : '#fff'
    const icon = logoPath
      ? L.divIcon({
          html: `<div style="background:${bg};border-radius:3px;box-shadow:0 1px 4px rgba(0,0,0,0.6);padding:2px 4px;display:flex;align-items:center;justify-content:center;width:40px;height:28px;overflow:hidden;"><img src="${BASE_URL}${logoPath}" alt="" style="width:36px;height:22px;object-fit:contain;display:block;" /></div>`,
          className: '',
          iconSize: [40, 28],
          iconAnchor: [20, 14]
        })
      : fallbackIcon

    const name = formatStoreName(s.chain, s.store_name)
    const chain = chainLabel(s.chain)
    const addressLine = s.address ? `<div class="mp-address">${s.address}</div>` : ''
    const cityLine = (s.city || s.province)
      ? `<div class="mp-address">${[s.city, s.province].filter(Boolean).join(', ')}</div>`
      : ''
    const popup = `
      <div class="mp-popup">
        <div class="mp-name">${name}</div>
        <div class="mp-chain">${chain}</div>
        ${addressLine}
        ${cityLine}
        <div class="mp-dist">${s.distanceKm} km away</div>
      </div>`.trim()

    L.marker([s.lat, s.lon], { icon })
      .bindPopup(popup, { maxWidth: 220 })
      .addTo(storeLayerGroup)
  }
}

async function handlePostalCode(code) {
  if (!code || code.length < 6) return
  const coords = await geocodePostalCode(code)
  if (coords) {
    user.setLatlng(coords)
    await initMap(coords)
  }
}

onMounted(async () => {
  if (props.postalCode) {
    await handlePostalCode(props.postalCode)
  } else if (user.latlng) {
    await initMap(user.latlng)
  }
})

watch(() => props.postalCode, (code) => {
  if (code) handlePostalCode(code)
})

// Update the radius circle when the slider moves
watch(() => props.radiusKm, (km) => {
  if (!map || !user.latlng) return
  if (radiusCircle) {
    radiusCircle.setRadius(km * 1000)
  }
})

onUnmounted(() => {
  if (map) { map.remove(); map = null }
})
</script>

<template>
  <div class="map-wrap">
    <div v-if="geocoding" class="map-loading">
      <span class="map-spinner" />
      <span>Locating…</span>
    </div>
    <div ref="mapEl" class="map-canvas" />
  </div>
</template>

<style>
/* ── Leaflet popup dark theme ─────────────────────────────────────────────── */
.leaflet-popup-content-wrapper {
  background: #1e3322 !important;
  color: #f0ede6 !important;
  border: 1px solid rgba(255,255,255,0.1) !important;
  border-radius: 6px !important;
  box-shadow: 0 4px 16px rgba(0,0,0,0.5) !important;
  padding: 0 !important;
}

.leaflet-popup-content {
  margin: 0 !important;
  line-height: 1.4 !important;
}

.leaflet-popup-tip {
  background: #1e3322 !important;
}

.leaflet-popup-close-button {
  color: rgba(240,237,230,0.5) !important;
  top: 6px !important;
  right: 8px !important;
}

.leaflet-popup-close-button:hover {
  color: #f0ede6 !important;
}

.mp-popup {
  padding: 12px 14px;
  min-width: 150px;
}

.mp-name {
  font-weight: 700;
  font-size: 0.9rem;
  color: #f0ede6;
  margin-bottom: 2px;
  line-height: 1.3;
}

.mp-chain {
  font-size: 0.75rem;
  color: #f0a500;
  font-weight: 600;
  letter-spacing: 0.03em;
  margin-bottom: 6px;
}

.mp-address {
  font-size: 0.75rem;
  color: rgba(240,237,230,0.65);
  margin-bottom: 1px;
}

.mp-dist {
  font-size: 0.72rem;
  color: rgba(240,237,230,0.45);
  margin-top: 6px;
  border-top: 1px solid rgba(255,255,255,0.08);
  padding-top: 5px;
}

/* Global — Leaflet dot marker */
.map-dot {
  width: 20px;
  height: 20px;
  border-radius: 50%;
  background: var(--c-amber, #f0a500);
  border: 3px solid #fff;
  box-shadow: 0 0 0 0 rgba(240, 165, 0, 0.6);
  animation: pulse-dot 2s infinite;
}

.store-dot {
  width: 10px;
  height: 10px;
  border-radius: 50%;
  background: var(--c-green, #4caf7d);
  border: 2px solid #fff;
  opacity: 0.85;
}

@keyframes pulse-dot {
  0%   { box-shadow: 0 0 0 0 rgba(240,165,0,0.6); }
  70%  { box-shadow: 0 0 0 14px rgba(240,165,0,0); }
  100% { box-shadow: 0 0 0 0 rgba(240,165,0,0); }
}
</style>

<style scoped>
.map-wrap {
  position: relative;
  width: 100%;
  height: 100%;
  min-height: 400px;
  border-radius: 4px;
  overflow: hidden;
}

.map-canvas {
  width: 100%;
  height: 100%;
  min-height: 400px;
}

.map-loading {
  position: absolute;
  inset: 0;
  z-index: 10;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 12px;
  background: rgba(27, 45, 30, 0.8);
  color: var(--c-ivory);
  font-family: var(--font-body);
  font-size: 0.85rem;
  letter-spacing: 0.06em;
}

.map-spinner {
  display: block;
  width: 28px;
  height: 28px;
  border: 2px solid var(--c-border);
  border-top-color: var(--c-amber);
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}
</style>
