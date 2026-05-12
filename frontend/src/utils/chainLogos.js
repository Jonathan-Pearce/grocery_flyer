/**
 * Maps chain slugs to local logo image paths (relative to public/).
 * Any chain not listed here falls back to the text pill.
 */
export const CHAIN_LOGO_URLS = {
  // Loblaws group
  loblaws:                  'images/chains/Loblaws_Brand_Logo.svg.png',
  nofrills:                 'images/chains/No_Frills_logo.svg.png',
  real_canadian_superstore: 'images/chains/Real_Canadian_Superstore_logo.svg.png',
  independent_city_market:  'images/chains/LCM.png',
  // Sobeys group
  sobeys:                   'images/chains/Sobeys_Corporate_Logo_2024.svg.png',
  freshco:                  'images/chains/FRESHCO-1.png.webp',
  farm_boy:                 'images/chains/Farm_Boy_logo.svg.png',
  longos:                   'images/chains/longos.png',
  // Walmart
  walmart:                  'images/chains/walmart.png',
  // Metro group
  metro:                    'images/chains/Metro_Inc._logo.svg.png',
  food_basics:              'images/chains/food-basics.png',
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
