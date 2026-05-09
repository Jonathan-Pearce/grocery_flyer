import { ref } from 'vue'

const NOMINATIM = 'https://nominatim.openstreetmap.org/search'

export function useGeocoding() {
  const coords = ref(null)
  const geocoding = ref(false)
  const geocodeError = ref(null)

  async function geocodePostalCode(postalCode) {
    geocoding.value = true
    geocodeError.value = null
    try {
      const params = new URLSearchParams({
        postalcode: postalCode.replace(/\s/g, ''),
        country: 'Canada',
        format: 'json',
        limit: '1'
      })
      const res = await fetch(`${NOMINATIM}?${params}`, {
        headers: { 'User-Agent': 'FlyerDeals/0.1 (grocery-flyer-app)' }
      })
      if (!res.ok) throw new Error('Geocoding request failed')
      const data = await res.json()
      if (!data.length) throw new Error('Postal code not found')
      coords.value = { lat: parseFloat(data[0].lat), lng: parseFloat(data[0].lon) }
    } catch (err) {
      geocodeError.value = err.message
      // Fallback to approximate centre of Canada — won't happen in practice
      coords.value = { lat: 56.1304, lng: -106.3468 }
    } finally {
      geocoding.value = false
    }
    return coords.value
  }

  return { coords, geocoding, geocodeError, geocodePostalCode }
}
