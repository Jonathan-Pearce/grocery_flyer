import { defineStore } from 'pinia'
import { ref } from 'vue'

export const useRankingsStore = defineStore('rankings', () => {
  const chains = ref([])
  const flyers = ref([])
  const history = ref([])   // [{ week_label, chains: [] }, ...]
  const isLoading = ref(false)
  const error = ref(null)

  async function loadRankings() {
    if (chains.value.length > 0) return
    isLoading.value = true
    error.value = null
    const base = import.meta.env.BASE_URL
    try {
      const [rankRes, histRes] = await Promise.all([
        fetch(`${base}data/rankings.json`),
        fetch(`${base}data/rankings_history.json`),
      ])
      if (!rankRes.ok) throw new Error('Failed to load rankings data')
      const data = await rankRes.json()
      chains.value = data.chains ?? []
      flyers.value = data.flyers ?? []
      if (histRes.ok) history.value = await histRes.json()
    } catch (err) {
      error.value = err.message
    } finally {
      isLoading.value = false
    }
  }

  return { chains, flyers, history, isLoading, error, loadRankings }
})
