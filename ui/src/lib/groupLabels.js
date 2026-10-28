/**
 * Plan 2: display labels for the document group (state / portal tag).
 * Authorization still comes from Keycloak JWT — this is UI only.
 */

export const PORTAL_GROUP = 'bv'

/** Short badge label: mh → MH, bv → BV */
export function formatGroupLabel(group) {
  const code = String(group || '')
    .trim()
    .toLowerCase()
  if (!code || code === 'default') return null
  if (code === 'bv' || code === 'bharat-vistaar' || code === 'bharat_vistaar') {
    return 'BV'
  }
  return code.toUpperCase()
}

export function groupBadgeTitle(group) {
  const code = String(group || '')
    .trim()
    .toLowerCase()
  if (!code || code === 'default') return 'No state tag'
  if (code === 'bv' || code === 'bharat-vistaar' || code === 'bharat_vistaar') {
    return 'Bharat Vistaar portal'
  }
  return `State: ${code.toUpperCase()}`
}

/**
 * Default group for upload form.
 * Super admin → portal (bv); single state → that state; multi → first (user can change).
 */
export function defaultUploadGroup({ isSuperAdmin, instances = [], portalGroup = PORTAL_GROUP }) {
  if (isSuperAdmin) return portalGroup || PORTAL_GROUP
  const list = (instances || []).map((i) => String(i).trim().toLowerCase()).filter(Boolean)
  if (list.length >= 1) return list[0]
  return ''
}
