/**
 * Emoji icons for grocery category_l1 values.
 * Used in DealCard and CategoryFilter.
 */
export const CATEGORY_ICONS = {
  'Produce':                      '🥦',
  'Meat & Seafood':               '🥩',
  'Dairy & Eggs':                 '🥛',
  'Bakery':                       '🍞',
  'Pantry':                       '🫙',
  'Beverages':                    '☕',
  'Snacks & Confectionery':       '🍿',
  'Frozen':                       '❄️',
  'Deli & Prepared Foods':        '🥪',
  'Health & Beauty':              '💊',
  'Household':                    '🏠',
  'Pet':                          '🐾',
  'Baby & Infant':                '👶',
  'Apparel & General Merchandise':'👕',
  'Other':                        '📦',
}

export function categoryIcon(name) {
  return CATEGORY_ICONS[name] ?? ''
}
