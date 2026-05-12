import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

/** Maps the first letter of a Canadian postal code to its province */
const POSTAL_PREFIX_MAP = {
  A: 'NL', B: 'NS', C: 'PE', E: 'NB',
  G: 'QC', H: 'QC', J: 'QC',
  K: 'ON', L: 'ON', M: 'ON', N: 'ON', P: 'ON',
  R: 'MB', S: 'SK', T: 'AB', V: 'BC',
  X: 'NT', Y: 'YT'
}

const STORAGE_KEY = 'flyerdeals_user'

function loadFromStorage() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    return raw ? JSON.parse(raw) : null
  } catch {
    return null
  }
}

export const useUserStore = defineStore('user', () => {
  const saved = loadFromStorage()

  const postalCode = ref(saved?.postalCode ?? '')
  const province = ref(saved?.province ?? '')
  const latlng = ref(saved?.latlng ?? null)
  // selectedStoreCodes: Set of "chain:store_code" strings (e.g. "loblaws:1024")
  const selectedStoreCodes = ref(new Set(saved?.selectedStoreCodes ?? []))

  const hasLocation = computed(() => !!postalCode.value && !!province.value)
  const hasStores = computed(() => selectedStoreCodes.value.size > 0)
  // backward-compat alias used by AppHeader
  const hasChains = hasStores

  function persist() {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({
      postalCode: postalCode.value,
      province: province.value,
      latlng: latlng.value,
      selectedStoreCodes: [...selectedStoreCodes.value],
    }))
  }

  function setPostalCode(code) {
    const clean = code.trim().toUpperCase().replace(/\s/g, '')
    postalCode.value = clean
    const prefix = clean[0]
    province.value = POSTAL_PREFIX_MAP[prefix] ?? ''
    latlng.value = null
    selectedStoreCodes.value = new Set()
    persist()
  }

  function setLatlng(coords) {
    latlng.value = coords
    persist()
  }

  /** Toggle a single store in/out of the selection. */
  function toggleStoreCode(chain, storeCode) {
    const key = `${chain}:${storeCode}`
    if (selectedStoreCodes.value.has(key)) {
      selectedStoreCodes.value.delete(key)
    } else {
      selectedStoreCodes.value.add(key)
    }
    persist()
  }

  /** Replace the entire selection with the provided array of store objects (each has .chain + .store_code). */
  function setStoreCodesFromRadius(stores) {
    selectedStoreCodes.value = new Set(stores.map(s => `${s.chain}:${s.store_code}`))
    persist()
  }

  function clearStoreCodes() {
    selectedStoreCodes.value = new Set()
    persist()
  }

  function reset() {
    postalCode.value = ''
    province.value = ''
    latlng.value = null
    selectedStoreCodes.value = new Set()
    localStorage.removeItem(STORAGE_KEY)
  }

  return {
    postalCode, province, latlng, selectedStoreCodes,
    hasLocation, hasStores, hasChains,
    setPostalCode, setLatlng,
    toggleStoreCode, setStoreCodesFromRadius, clearStoreCodes,
    reset,
  }
})
