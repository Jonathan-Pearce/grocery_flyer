import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { useUserStore } from './user.js'

export const useDealsStore = defineStore('deals', () => {
  const rawDeals = ref([])
  const isLoading = ref(false)
  const error = ref(null)
  const activeCategory = ref(null)

  async function loadDeals() {
    if (rawDeals.value.length > 0) return
    isLoading.value = true
    error.value = null
    try {
      const res = await fetch('/data/active_scores.json')
      if (!res.ok) throw new Error('Failed to load deal data')
      rawDeals.value = await res.json()
    } catch (err) {
      error.value = err.message
    } finally {
      isLoading.value = false
    }
  }

  const filteredDeals = computed(() => {
    const user = useUserStore()
    const chains = user.selectedChains

    return rawDeals.value
      .filter(d => chains.size === 0 || chains.has(d.store_chain))
      .filter(d => !activeCategory.value || d.category_l1 === activeCategory.value)
      .sort((a, b) => (b.deal_score ?? 0) - (a.deal_score ?? 0))
  })

  const categories = computed(() => {
    const user = useUserStore()
    const chains = user.selectedChains
    const seen = new Set()
    rawDeals.value
      .filter(d => chains.size === 0 || chains.has(d.store_chain))
      .forEach(d => d.category_l1 && seen.add(d.category_l1))
    return [...seen].sort()
  })

  function setCategory(cat) {
    activeCategory.value = cat === activeCategory.value ? null : cat
  }

  return {
    rawDeals, isLoading, error, activeCategory,
    loadDeals, filteredDeals, categories, setCategory
  }
})
