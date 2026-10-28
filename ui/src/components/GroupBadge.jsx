import { formatGroupLabel, groupBadgeTitle } from '../lib/groupLabels'
import { Badge } from './ui/badge'
import { cn } from '../lib/utils'

/** Plan 2: show document state / BV portal tag from ``group``. */
export function GroupBadge({ group, className }) {
  const label = formatGroupLabel(group)
  if (!label) return null
  const isPortal = label === 'BV'
  return (
    <Badge
      variant={isPortal ? 'default' : 'outline'}
      title={groupBadgeTitle(group)}
      className={cn('text-[10px] font-semibold tracking-wide', className)}
    >
      {label}
    </Badge>
  )
}

export default GroupBadge
