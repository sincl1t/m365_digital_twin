
# Dashboard Blinking Fix Plan

## Problem Analysis
The dashboard blinks in Live (MQTT) mode due to:
1. **Unconditional page refresh**: `time.sleep(refresh_sec)` followed by `st.rerun()` at the end of the script causes the entire page to reload periodically
2. **Blocking data collection**: The `df_from_queue` function blocks for `refresh_sec` seconds waiting for MQTT data
3. **Complete UI re-rendering**: Every refresh causes all charts and metrics to re-render, causing visible flickering

## Solution Plan
1. **Remove unconditional rerun**: Eliminate the `time.sleep()` + `st.rerun()` pattern
2. **Implement proper caching**: Use Streamlit's caching mechanisms to avoid unnecessary data reprocessing
3. **Optimize MQTT data handling**: Process MQTT data more efficiently without blocking
4. **Use st_autorefresh**: Replace manual refresh with Streamlit's automatic refresh functionality
5. **Optimize chart updates**: Only update charts when new data is available



## Implementation Steps
- [x] Modify the main loop to use st_autorefresh instead of manual rerun
- [x] Cache the MQTT data processing functions
- [x] Optimize the data loading functions for non-blocking operation
- [x] Add proper error handling for MQTT connection issues
- [x] Test the fixed version to ensure blinking is eliminated
- [x] Update requirements.txt to include streamlit-autorefresh dependency

## Files Modified
- `streamlit_app.py` - Main application file (COMPLETED)
- `requirements.txt` - Added streamlit-autorefresh dependency (COMPLETED)

## Key Changes Made
1. **Added st_autorefresh**: Replaced manual sleep/rerun with Streamlit's automatic refresh component
2. **Added caching**: Applied `@st.cache_data` decorators to data loading functions for better performance
3. **Non-blocking MQTT data collection**: Modified `df_from_queue` to use short timeouts instead of blocking
4. **Removed manual refresh loop**: Eliminated the problematic `time.sleep()` + `st.rerun()` pattern
5. **Smart cache clearing**: Cache is only cleared for live modes when autorefresh triggers
6. **Added dependencies**: Updated requirements.txt to include streamlit-autorefresh package
