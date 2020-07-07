from datetime import datetime

def next_month(sourcedate):
  month = sourcedate.month + 1																								# Get next month
  if month == 13:																															# If month is 13, then must increment year
    return datetime(sourcedate.year+1, 1, 1)																	# Create new date with incremented year, month of one (1), day of one (1)
  return datetime(sourcedate.year, month, 1)																	# If here, then month was less than 13, keep year, use incremented month, day of one (1)
