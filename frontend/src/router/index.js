import { createRouter, createWebHashHistory } from 'vue-router'
import HomeView from '@/views/HomeView.vue'
import DealsView from '@/views/DealsView.vue'
import RankingsView from '@/views/RankingsView.vue'

export const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    { path: '/', component: HomeView, name: 'home' },
    { path: '/deals', component: DealsView, name: 'deals' },
    { path: '/rankings', component: RankingsView, name: 'rankings' },
  ]
})
