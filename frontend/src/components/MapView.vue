<script setup>
import { onMounted, onUnmounted, watch, ref } from 'vue'
import { useUserStore } from '@/stores/user.js'
import { useGeocoding } from '@/composables/useGeocoding.js'
import { useStores } from '@/composables/useStores.js'

const props = defineProps({
  postalCode: { type: String, default: '' }
})

const user = useUserStore()
const { geocodePostalCode, geocoding } = useGeocoding()
const { nearbyStores } = useStores()

const mapEl = ref(null)
let map = null
let marker = null
let storeLayerGroup = null

async function initMap(coords) {
  // Lazy-import Leaflet so it only loads when map mounts
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

  await addStoreMarkers(L, coords)
}

async function addStoreMarkers(L, coords) {
  if (!storeLayerGroup) return
  storeLayerGroup.clearLayers()

  const stores = await nearbyStores(coords.lat, coords.lng, 50)

  const storeIcon = L.divIcon({
    html: `<div class="store-dot"></div>`,
    className: '',
    iconSize: [12, 12],
    iconAnchor: [6, 6]
  })

  for (const s of stores) {
    const label = `<b>${s.store_name}</b><br>${s.chain} &mdash; ${s.distanceKm} km`
    L.marker([s.lat, s.lon], { icon: storeIcon })
      .bindPopup(label)
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
