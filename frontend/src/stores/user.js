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
  const selectedChains = ref(new Set(saved?.selectedChains ?? []))

  const hasLocation = computed(() => !!postalCode.value && !!province.value)
  const hasChains = computed(() => selectedChains.value.size > 0)

  function persist() {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({
      postalCode: postalCode.value,
      province: province.value,
      latlng: latlng.value,
      selectedChains: [...selectedChains.value]
    }))
  }

  function setPostalCode(code) {
    const clean = code.trim().toUpperCase().replace(/\s/g, '')
    postalCode.value = clean
    const prefix = clean[0]
    province.value = POSTAL_PREFIX_MAP[prefix] ?? ''
    latlng.value = null
    selectedChains.value = new Set()
    persist()
  }

  function setLatlng(coords) {
    latlng.value = coords
    persist()
  }

  function toggleChain(chainId) {
    if (selectedChains.value.has(chainId)) {
      selectedChains.value.delete(chainId)
    } else {
      selectedChains.value.add(chainId)
    }
    persist()
  }

  function clearChains() {
    selectedChains.value = new Set()
    persist()
  }

  function reset() {
    postalCode.value = ''
    province.value = ''
    latlng.value = null
    selectedChains.value = new Set()
    localStorage.removeItem(STORAGE_KEY)
  }

  return {
    postalCode, province, latlng, selectedChains,
    hasLocation, hasChains,
    setPostalCode, setLatlng, toggleChain, clearChains, reset
  }
})
