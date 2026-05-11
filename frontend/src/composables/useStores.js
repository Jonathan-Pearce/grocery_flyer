import { ref } from 'vue'

/** @type {import('vue').Ref<Array|null>} */
const storeData = ref(null)
let loadPromise = null

function haversineKm(lat1, lon1, lat2, lon2) {
  const R = 6371
  const dLat = ((lat2 - lat1) * Math.PI) / 180
  const dLon = ((lon2 - lon1) * Math.PI) / 180
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) ** 2
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
}

async function loadStores() {
  if (storeData.value) return storeData.value
  if (loadPromise) return loadPromise

  loadPromise = fetch(import.meta.env.BASE_URL + 'data/stores_geo.json')
    .then((r) => r.json())
    .then((data) => {
      // Drop stores with no coordinates (e.g. Adonis online storefront)
      storeData.value = data.filter((s) => s.lat != null && s.lon != null)
      return storeData.value
    })
    .catch(() => {
      storeData.value = []
      return []
    })
  return loadPromise
}

export function useStores() {
  /**
   * Returns stores within `radiusKm` of the given coordinates, sorted by distance.
   * Each returned store has an added `distanceKm` (number, 1 decimal).
   */
  async function nearbyStores(userLat, userLng, radiusKm = 50) {
    const stores = await loadStores()
    return stores
      .map((s) => ({
        ...s,
        distanceKm: Math.round(haversineKm(userLat, userLng, s.lat, s.lon) * 10) / 10,
      }))
      .filter((s) => s.distanceKm <= radiusKm)
      .sort((a, b) => a.distanceKm - b.distanceKm)
  }

  return { nearbyStores }
}
