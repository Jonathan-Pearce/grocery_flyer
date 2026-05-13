/**
 * Maps chain slugs to local logo image paths (relative to public/).
 * Any chain not listed here falls back to the text pill.
 */
export const CHAIN_LOGO_URLS = {
  // Loblaws group
  loblaws:                  'images/chains/loblaws.png',
  nofrills:                 'images/chains/nofrills.png',
  provigo:                  'images/chains/provigo.png',
  real_canadian_superstore: 'images/chains/real_canadian_superstore.png',
  maxi:                     'images/chains/maxi.png',
  zehrs:                    'images/chains/zehrs.png',
  fortinos:                 'images/chains/fortinos.png',
  atlantic_superstore:      'images/chains/atlantic_superstore.png',
  dominion:                 'images/chains/dominion.webp',
  independent_grocer:       'images/chains/independent_grocer.png',
  independent_city_market:  'images/chains/independent_city_market.png',
  freshmart:                'images/chains/freshmart.png',
  // Sobeys group
  sobeys:                   'images/chains/sobeys.png',
  safeway:                  'images/chains/safeway.png',
  iga:                      'images/chains/iga.jpg',
  freshco:                  'images/chains/freshco.png',
  foodland:                 'images/chains/foodland.png',
  longos:                   'images/chains/longos.png',
  farm_boy:                 'images/chains/farm_boy.png',
  // Walmart
  walmart:                  'images/chains/walmart.png',
  // Metro group
  metro:                    'images/chains/metro.png',
  metro_qc:                 'images/chains/metro.png',
  food_basics:              'images/chains/food_basics.png',
  super_c:                  'images/chains/super_c.png',
  adonis:                   'images/chains/adonis.png',
}

/**
 * Chains whose logos have a transparent background with light/yellow content —
 * these need a dark pill instead of the default white one.
 */
export const DARK_BG_LOGOS = new Set(['food_basics'])

/** Display labels for chain slugs */
export const CHAIN_LABELS = {
  adonis:                  'Adonis',
  atlantic_superstore:     'Atlantic Superstore',
  dominion:                'Dominion',
  farm_boy:                'Farm Boy',
  food_basics:             'Food Basics',
  foodland:                'Foodland',
  fortinos:                'Fortinos',
  freshco:                 'FreshCo',
  freshmart:               'Freshmart',
  iga:                     'IGA',
  independent_city_market: 'City Market',
  independent_grocer:      'Independent Grocer',
  loblaws:                 'Loblaws',
  longos:                  "Longo's",
  maxi:                    'Maxi',
  metro:                   'Metro',
  metro_qc:                'Metro QC',
  nofrills:                'No Frills',
  provigo:                 'Provigo',
  real_canadian_superstore:'Real Canadian',
  safeway:                 'Safeway',
  sobeys:                  'Sobeys',
  super_c:                 'Super C',
  walmart:                 'Walmart',
  zehrs:                   'Zehrs',
}

export function chainLabel(slug) {
  return CHAIN_LABELS[slug] ?? slug?.replace(/_/g, ' ')
}

export function chainLogoUrl(slug) {
  return CHAIN_LOGO_URLS[slug] ?? null
}
