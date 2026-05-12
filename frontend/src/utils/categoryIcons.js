/**
 * Emoji icons for grocery category_l1 values.
 * Used in DealCard and CategoryFilter.
 */
export const CATEGORY_ICONS = {
  'Produce':         '🥦',
  'Meat':            '🥩',
  'Seafood':         '🐟',
  'Dairy & Eggs':    '🥛',
  'Bakery & Bread':  '🍞',
  'Pantry':          '🫙',
  'Beverages':       '☕',
  'Snacks':          '🍿',
  'Frozen':          '❄️',
  'Deli & Prepared': '🥪',
}

export function categoryIcon(name) {
  return CATEGORY_ICONS[name] ?? ''
}
