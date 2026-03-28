const Anthropic = require('@anthropic-ai/sdk');
const config = require('./config');

let anthropicClient = null;

function getAnthropicClient() {
  if (!anthropicClient) {
    anthropicClient = new Anthropic({ apiKey: config.anthropic.apiKey });
  }
  return anthropicClient;
}

const SYSTEM_PROMPT = `You are a cellular network security analyst specializing in RF threat detection and IMSI catcher / stingray identification.

Analyze the following cellular measurement anomalies and determine if they represent real threats.

For each event, evaluate the combination of signals:
- Unknown cells + signal anomalies = likely rogue base station
- Network downgrade + PLMN mismatch = classic IMSI catcher behavior
- Cluster anomalies across multiple devices = coordinated jamming or interception
- Timing advance anomalies = distance spoofing
- Known bad measurement matches = confirmed system-flagged issues

Return ONLY a valid JSON array with objects:
{
  "sample_id": string,
  "is_threat": boolean,
  "threat_type": string,
  "confidence": number (0-1),
  "reasoning": string
}

threat_type must be one of: IMSI_CATCHER, ROGUE_BASE_STATION, JAMMING, DOWNGRADE_ATTACK, SIGNAL_SPOOFING, DISTANCE_SPOOFING, UNKNOWN_THREAT

No other text — only the JSON array.`;

/**
 * Escalate high-suspicion flagged events to Claude for AI analysis.
 *
 * @param {Array} flaggedEvents - combined rule + stat flags above threshold
 * @returns {Array} confirmed threats with confidence > 0.7
 */
async function escalateToAI(flaggedEvents) {
  const threshold = config.agent.suspicionThreshold;
  const eligible = flaggedEvents.filter((e) => e.score > threshold);

  if (eligible.length === 0) {
    console.log('[AI] No events above suspicion threshold for escalation');
    return [];
  }

  const batches = [];
  for (let i = 0; i < eligible.length; i += config.anthropic.maxEventsPerBatch) {
    batches.push(eligible.slice(i, i + config.anthropic.maxEventsPerBatch));
  }

  console.log(`[AI] Escalating ${eligible.length} events in ${batches.length} batch(es)`);

  const confirmedThreats = [];

  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    try {
      const client = getAnthropicClient();
      const response = await client.messages.create({
        model: config.anthropic.model,
        max_tokens: 4096,
        system: SYSTEM_PROMPT,
        messages: [
          {
            role: 'user',
            content: JSON.stringify(batch, null, 2),
          },
        ],
      });

      const text = response.content
        .filter((c) => c.type === 'text')
        .map((c) => c.text)
        .join('');

      // Parse JSON response — handle potential markdown code fences
      let parsed;
      try {
        const cleaned = text.replace(/```json?\n?/g, '').replace(/```/g, '').trim();
        parsed = JSON.parse(cleaned);
      } catch (parseErr) {
        console.error(`[AI] Failed to parse batch ${i + 1} response:`, parseErr.message);
        console.error('[AI] Raw response:', text.substring(0, 500));
        continue;
      }

      if (!Array.isArray(parsed)) {
        console.error(`[AI] Batch ${i + 1} response is not an array`);
        continue;
      }

      // Filter to confirmed threats above confidence threshold
      const confirmed = parsed.filter(
        (t) => t.is_threat === true && t.confidence > config.anthropic.confirmationThreshold
      );

      // Enrich confirmed threats with original flag data
      for (const threat of confirmed) {
        const original = batch.find((e) => e.sample_id === threat.sample_id);
        if (original) {
          confirmedThreats.push({
            sample_id: threat.sample_id,
            cell_id: original.cell_id || '',
            cell_ecgi: original.cell_ecgi || '',
            location_lat: original.location_lat || 0,
            location_lng: original.location_lng || 0,
            threat_type: threat.threat_type,
            severity: original.severity || 'HIGH',
            score: original.score || 0,
            confidence: threat.confidence,
            reasoning: threat.reasoning,
            raw_flags: original.raw_flags || [],
            is_confirmed: 1,
          });
        }
      }

      console.log(`[AI] Batch ${i + 1}: ${confirmed.length}/${batch.length} confirmed as threats`);
    } catch (err) {
      console.error(`[AI] Batch ${i + 1} API call failed:`, err.message);
    }
  }

  return confirmedThreats;
}

module.exports = { escalateToAI };
