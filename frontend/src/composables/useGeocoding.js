import { ref } from 'vue'

// Module-level cache — loaded once, shared across all composable instances
let centroidsCache = null
let centroidsPromise = null

async function loadCentroids() {
  if (centroidsCache) return centroidsCache
  if (centroidsPromise) return centroidsPromise
  centroidsPromise = fetch(import.meta.env.BASE_URL + 'data/postal_centroids.json')
    .then(r => r.json())
    .then(data => { centroidsCache = data; return data })
    .catch(() => { centroidsCache = {}; return {} })
  return centroidsPromise
}

export function useGeocoding() {
  const coords = ref(null)
  const geocoding = ref(false)
  const geocodeError = ref(null)

  async function geocodePostalCode(postalCode) {
    geocoding.value = true
    geocodeError.value = null
    try {
      const fsa = postalCode.trim().toUpperCase().replace(/\s/g, '').slice(0, 3)
      const centroids = await loadCentroids()
      const entry = centroids[fsa]
      if (!entry) throw new Error(`Postal code area "${fsa}" not found`)
      coords.value = { lat: entry[0], lng: entry[1] }
    } catch (err) {
      geocodeError.value = err.message
      coords.value = null
    } finally {
      geocoding.value = false
    }
    return coords.value
  }

  return { coords, geocoding, geocodeError, geocodePostalCode }
}
