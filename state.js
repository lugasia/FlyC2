const db = require('./db');

let cachedWatermark = null;

async function loadWatermark() {
  try {
    const result = await db.getLastWatermark();
    if (result) {
      cachedWatermark = result;
      return cachedWatermark;
    }
  } catch (err) {
    console.error('[STATE] Failed to load watermark from DB:', err.message);
  }

  // Default: 1 hour ago on first run
  const defaultTs = new Date(Date.now() - 60 * 60 * 1000).toISOString();
  cachedWatermark = defaultTs;
  console.log(`[STATE] No watermark found, defaulting to ${defaultTs}`);
  return cachedWatermark;
}

async function saveWatermark(ts) {
  try {
    await db.updateWatermark(ts);
    cachedWatermark = ts;
    console.log(`[STATE] Watermark updated to ${ts}`);
  } catch (err) {
    console.error('[STATE] Failed to save watermark:', err.message);
  }
}

function getCurrentWatermark() {
  return cachedWatermark;
}

module.exports = { loadWatermark, saveWatermark, getCurrentWatermark };
