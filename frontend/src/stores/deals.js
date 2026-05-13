import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { useUserStore } from './user.js'

export const useDealsStore = defineStore('deals', () => {
  const rawDeals = ref([])
  const flyerRegions = ref([])
  const isLoading = ref(false)
  const error = ref(null)
  const activeCategory = ref(null)
  const activeTier = ref(null) // null | 'good' | 'hot'
  const searchQuery = ref('')

  async function parseDealsResponse(response) {
    const enc = response.headers.get('content-encoding') || ''
    if (enc.includes('gzip')) {
      return response.json()
    }

    // In static hosting, .gz files are often served as raw bytes.
    if (typeof DecompressionStream !== 'undefined' && response.body) {
      try {
        const ds = new DecompressionStream('gzip')
        return await new Response(response.body.pipeThrough(ds)).json()
      } catch {
        // Fall through to plain JSON parsing for already-decoded responses.
      }
    }

    return response.json()
  }

  async function loadDeals() {
    if (rawDeals.value.length > 0) return
    isLoading.value = true
    error.value = null
    const base = import.meta.env.BASE_URL
    try {
      const dealsGzPath = `${base}data/active_scores.json.gz`
      const dealsJsonPath = `${base}data/active_scores.json`
      const [initialDealsRes, regionsRes] = await Promise.all([
        fetch(dealsGzPath),
        fetch(`${base}data/flyer_regions.json`),
      ])

      let dealsRes = initialDealsRes
      if (!dealsRes.ok) {
        dealsRes = await fetch(dealsJsonPath)
      }
      if (!dealsRes.ok) throw new Error('Failed to load deal data')

      try {
        rawDeals.value = await parseDealsResponse(dealsRes)
      } catch {
        // Browser may not support DecompressionStream for raw .gz payloads.
        const fallbackRes = await fetch(dealsJsonPath)
        if (!fallbackRes.ok) throw new Error('Failed to load deal data')
        rawDeals.value = await fallbackRes.json()
      }
      if (regionsRes.ok) flyerRegions.value = await regionsRes.json()
    } catch (err) {
      error.value = err.message
    } finally {
      isLoading.value = false
    }
  }

  /**
   * Build the set of flyer_ids that are relevant to the user's selected stores.
   *
   * Each region in flyer_regions.json has { chain, region_id, store_codes[] }.
   * A region matches if the user has selected at least one store from that chain
   * whose store_code appears in the region's store_codes list.
   *
   * region_id == flyer_id in active_scores.json.
   */
  const matchedFlyerIds = computed(() => {
    const user = useUserStore()
    if (user.selectedStoreCodes.size === 0) return null

    // Build a quick lookup: chain → Set<store_code>
    const byChain = {}
    for (const key of user.selectedStoreCodes) {
      const [chain, code] = key.split(':')
      if (!byChain[chain]) byChain[chain] = new Set()
      byChain[chain].add(code)
    }

    const ids = new Set()
    for (const region of flyerRegions.value) {
      const selected = byChain[region.chain]
      if (!selected) continue
      for (const code of region.store_codes) {
        if (selected.has(code)) {
          ids.add(region.region_id)
          break
        }
      }
    }
    return ids
  })

  const filteredDeals = computed(() => {
    const user = useUserStore()
    const ids = matchedFlyerIds.value

    return rawDeals.value
      .filter(d => {
        if (!ids) return false
        // If we have matched flyer IDs, use them; otherwise fall back to chain filter
        if (ids.size > 0) return ids.has(d.flyer_id)
        // Fallback: filter by chain (when flyer_regions has no match but chains are known)
        const selectedChains = new Set(
          [...user.selectedStoreCodes].map(k => k.split(':')[0])
        )
        return selectedChains.has(d.store_chain)
      })
      .filter(d => !activeCategory.value || d.category_l1 === activeCategory.value)
      .filter(d => {
        if (!activeTier.value) return true
        const s = d.deal_score ?? 0
        if (activeTier.value === 'hot')  return s >= 80
        if (activeTier.value === 'good') return s >= 65
        return true
      })
      .filter(d => {
        const q = searchQuery.value.trim().toLowerCase()
        if (!q) return true
        return (d.name_en ?? '').toLowerCase().includes(q) ||
               (d.name_fr ?? '').toLowerCase().includes(q) ||
               (d.brand  ?? '').toLowerCase().includes(q)
      })
      .sort((a, b) => (b.deal_score ?? 0) - (a.deal_score ?? 0))
      // Deduplicate: same chain + same product name + same price = same deal.
      // Keeps the highest-scored row (first after sort) when multiple stores
      // or flyer regions produce identical-looking entries.
      .filter((() => {
        const seen = new Set()
        return d => {
          const key = `${d.store_chain}\x00${d.name_en ?? d.name_fr ?? ''}\x00${d.sale_price ?? ''}`
          if (seen.has(key)) return false
          seen.add(key)
          return true
        }
      })())
      .slice(0, searchQuery.value.trim() ? Infinity : 50)
  })

  const categories = computed(() => {
    const user = useUserStore()
    const ids = matchedFlyerIds.value
    const seen = new Set()
    rawDeals.value
      .filter(d => {
        if (!ids) return false
        if (ids.size > 0) return ids.has(d.flyer_id)
        const selectedChains = new Set(
          [...user.selectedStoreCodes].map(k => k.split(':')[0])
        )
        return selectedChains.has(d.store_chain)
      })
      .forEach(d => d.category_l1 && seen.add(d.category_l1))
    return [...seen].sort()
  })

  function setCategory(cat) {
    activeCategory.value = cat === activeCategory.value ? null : cat
  }

  function setTier(tier) {
    activeTier.value = activeTier.value === tier ? null : tier
  }

  function setSearch(q) {
    searchQuery.value = q
  }

  return {
    rawDeals, flyerRegions, isLoading, error, activeCategory, activeTier, searchQuery,
    loadDeals, filteredDeals, categories, setCategory, setTier, setSearch,
  }
})
