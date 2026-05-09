import { createRouter, createWebHistory } from 'vue-router'
import HomeView from '@/views/HomeView.vue'
import DealsView from '@/views/DealsView.vue'

export const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', component: HomeView, name: 'home' },
    { path: '/deals', component: DealsView, name: 'deals' }
  ]
})
