/** Title-case a string, handling hyphens (e.g. ST-LIN → St-Lin) */
export function toTitleCase(str) {
  return str.toLowerCase().replace(/(^|[\s-])([a-z])/g, (_, sep, c) => sep + c.toUpperCase())
}

/** Return a cleaned display name for a store, stripping redundant chain prefixes/suffixes */
export function formatStoreName(chain, raw) {
  let s = (raw || '').trim()

  // Global: replace " @" used as "at" (e.g. "1323 @Hanna" → "1323 Hanna")
  s = s.replace(/\s*@\s*/g, ' ').trim()

  switch (chain) {
    case 'loblaws':
      s = s.replace(/^Loblaws\s*[-–]\s*/i, '').replace(/^Loblaws\s+/i, '')
      break
    case 'dominion':
      s = s.replace(/\s+Dominion$/i, '')
      break
    case 'atlantic_superstore':
    case 'real_canadian_superstore':
      s = s.replace(/\s+Superstore$/i, '')
      break
    case 'foodland':
      s = s.replace(/^Foodland\s+/i, '')
      break
    case 'freshmart':
      s = s.replace(/\s+Freshmart$/i, '')
      break
    case 'fortinos':
      s = s.replace(/^Fortinos\s+/i, '')
      break
    case 'iga':
      s = s.replace(/^IGA\s*[-–]?\s*/i, '')
      break
    case 'sobeys':
      s = s.replace(/^Sobeys\s+/i, '')
      break
    case 'zehrs':
      s = s.replace(/^Zehrs\s+/i, '')
      break
    case 'provigo':
      s = s.replace(/^Provigo\s+/i, '')
      break
    case 'metro_qc':
      s = s.replace(/^Metro\s+Plus\s+/i, '').replace(/^Metro\s+/i, '')
      break
    case 'metro':
      s = s.replace(/^#/, '')
      break
    case 'independent_city_market':
      s = s
        .replace(/^Independent\s+City\s+Market\s*@?\s*/i, '')
        .replace(/\s+Independent\s+City\s+Market$/i, '')
        .replace(/^Loblaw\s+City\s+Market\s*[-–]\s*/i, '')
      break
    case 'independent_grocer':
      s = s.replace(/\bYIG\b\s*/i, '')
      break
    case 'nofrills':
      // "Owner nofrills - Location" → "Owner · Location"
      s = s
        .replace(/\s+nofrills\s*[-–]\s*/i, ' · ')
        .replace(/\s+NF\s+/i, ' · ')
        .replace(/\s+nofrills\b/i, '')
        .replace(/\s+NF\s*$/i, '')
      break
    case 'food_basics':
    case 'super_c':
      s = toTitleCase(s)
      break
  }

  return s.replace(/\s{2,}/g, ' ').trim()
}
