const { createClient } = require('@supabase/supabase-js');
const fetch = require('node-fetch');

// Check for test mode from command line
const testMode = process.argv.includes('--test');

console.log('🏈 NFL Odds Alternate Lines Updater');
console.log('===============================================================================');

if (testMode) {
  console.log('🧪 TEST MODE: No database changes will be made');
}

// ─────────────────────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────────────────────
const ODDS_API_KEY = 'feda7e2063f85e1822c2623832e9eefc';
const SPORTS_KEY = 'americanfootball_nfl';
const BOOKMAKERS = 'draftkings,fanduel';
const ALTERNATE_MARKETS = [
  'player_pass_yds_alternate',
  'player_pass_tds_alternate',
  'player_pass_attempts_alternate',
  'player_pass_completions_alternate',
  'player_pass_interceptions_alternate',
  'player_rush_yds_alternate',
  'player_rush_attempts_alternate',
  'player_rush_tds_alternate',
  'player_rush_reception_yds_alternate',
  'player_reception_yds_alternate',
  'player_receptions_alternate',
  'player_field_goals_alternate'
];

// Get environment variables
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('❌ Error: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables are required');
  process.exit(1);
}

// Create Supabase client with service role
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// ─────────────────────────────────────────────────────────────
// Helper Functions
// ─────────────────────────────────────────────────────────────
function nextThursdayUtcStart(base) {
  const d = new Date(Date.UTC(base.getUTCFullYear(), base.getUTCMonth(), base.getUTCDate(), 0, 0, 0));
  const day = d.getUTCDay(); // Sun=0..Sat=6
  const offset = day <= 3 ? 4 - day : 11 - day; // to next Thu 00:00Z
  d.setUTCDate(d.getUTCDate() + offset);
  return d;
}

function getUpcomingWeekWindow(now = new Date()) {
  const start = nextThursdayUtcStart(now);
  const end = new Date(start);
  end.setUTCDate(end.getUTCDate() + 7);
  return { start, end };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function mapMarketToPropType(marketKey) {
  const m = {
    player_pass_yds_alternate: 'passing_yards',
    player_pass_tds_alternate: 'passing_touchdowns',
    player_pass_attempts_alternate: 'passing_attempts',
    player_pass_completions_alternate: 'passing_completions',
    player_pass_interceptions_alternate: 'pass_interceptions',
    player_rush_yds_alternate: 'rushing_yards',
    player_rush_attempts_alternate: 'rushing_attempts',
    player_rush_tds_alternate: 'rushing_touchdowns',
    player_rush_reception_yds_alternate: 'rush_reception_yards',
    player_reception_yds_alternate: 'receiving_yards',
    player_receptions_alternate: 'receptions',
    player_field_goals_alternate: 'field_goals'
  };
  return m[marketKey] || marketKey;
}

function determineBetType(name) {
  const n = (name || '').toLowerCase();
  if (n.includes('over')) return 'over';
  if (n.includes('under')) return 'under';
  return 'unknown';
}

function sanitize(s) {
  return String(s).replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_\-]/g, '');
}

function getSeasonYear(kickoff) {
  const m = kickoff.getUTCMonth();
  const y = kickoff.getUTCFullYear();
  return m >= 8 ? y : y - 1; // Sep–Feb window
}

function isInSeason(d) {
  const yr = d.getUTCFullYear();
  const start = Date.UTC(yr, 8, 1, 0, 0, 0); // Sep 1
  const end = Date.UTC(yr + 1, 1, 28, 23, 59, 59); // Feb 28 next yr
  const t = d.getTime();
  return t >= start && t <= end;
}

function toNumber(x) {
  if (x === null || x === undefined) return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

// ─────────────────────────────────────────────────────────────
// Main Updater Function
// ─────────────────────────────────────────────────────────────
async function runUpdater() {
  try {
    console.log('BUILD: v2-ThuWed-window (decimal_price only, outcome_price=NULL)');
    
    // Window = upcoming Thu→Wed (UTC), same as local script
    const now = new Date();
    const { start, end } = getUpcomingWeekWindow(now);
    console.log(`📅 Window (Thu→Wed): ${start.toISOString()} → ${end.toISOString()}`);
    
    // Optional in-season guard (bypass if testMode)
    if (!isInSeason(now) && !testMode) {
      return {
        success: true,
        message: 'Outside NFL season (skipped)',
        skipped: true
      };
    }
    
    // 1) Fetch all NFL events
    console.log('📡 Fetching NFL events from Odds API...');
    const eventsUrl = `https://api.the-odds-api.com/v4/sports/${SPORTS_KEY}/events?apiKey=${ODDS_API_KEY}`;
    const eventsRes = await fetch(eventsUrl);
    
    if (!eventsRes.ok) {
      throw new Error(`Events API failed: ${eventsRes.status}`);
    }
    
    const allEvents = await eventsRes.json();
    console.log(`✅ Found ${allEvents.length} events total`);
    
    // 2) Filter to Thu→Wed window
    const events = allEvents.filter((e) => {
      const t = new Date(e.commence_time);
      return t >= start && t < end;
    });
    console.log(`🎯 Filtered to ${events.length} events in upcoming window`);
    
    if (!events.length) {
      return {
        success: true,
        message: 'No games in upcoming window',
        inserted: 0
      };
    }
    
    let totalInserted = 0;
    let withData = 0;
    let noData = 0;
    
    for (const event of events) {
      const { id: eventId, home_team, away_team, commence_time } = event;
      const kickoff = new Date(commence_time);
      const seasonYear = getSeasonYear(kickoff);
      
      console.log(`\n— ${away_team} @ ${home_team} — ${kickoff.toUTCString()} [${seasonYear}]`);
      
      let eventLines = 0;
      
      for (const market of ALTERNATE_MARKETS) {
        const oddsUrl = `https://api.the-odds-api.com/v4/sports/${SPORTS_KEY}/events/${eventId}/odds?` +
          `apiKey=${ODDS_API_KEY}&regions=us&markets=${market}&bookmakers=${BOOKMAKERS}`;
        
        await sleep(400); // polite rate limit
        
        const oddsRes = await fetch(oddsUrl);
        if (!oddsRes.ok) {
          console.log(`   ⚠️ ${market}: ${oddsRes.status}`);
          continue;
        }
        
        const oddsData = await oddsRes.json();
        const books = Array.isArray(oddsData?.bookmakers) ? oddsData.bookmakers : [];
        const records = [];
        
        for (const book of books) {
          for (const mkt of book.markets ?? []) {
            const market_key = mkt.key;
            for (const out of mkt.outcomes ?? []) {
              if (!out?.description || out.point === undefined || out.point === null) continue;
              
              const decimal = toNumber(out.price); // store as decimal odds
              
              records.push({
                id: `${eventId}_${book.key}_${market_key}_${sanitize(out.description)}_${out.point}_${sanitize(out.name)}`,
                event_id: eventId,
                sport_key: SPORTS_KEY,
                commence_time,
                home_team,
                away_team,
                week_number: null,
                season_year: seasonYear,
                bookmaker_key: book.key,
                bookmaker_title: book.title,
                bookmaker_last_update: book.last_update,
                market_key,
                market_name: market_key,
                player_name: out.description,
                prop_type: mapMarketToPropType(market_key),
                outcome_name: out.name,
                outcome_price: null,
                decimal_price: decimal,
                line_value: out.point,
                bet_type: determineBetType(out.name),
                updated_at: new Date().toISOString()
              });
            }
          }
        }
        
        if (records.length) {
          if (!testMode) {
            try {
              const { error } = await supabase
                .from('nfl_odds_alternate_lines')
                .upsert(records, {
                  onConflict: 'event_id,bookmaker_key,market_key,player_name,line_value,outcome_name'
                });
              
              if (error) {
                console.log(`   ❌ Upsert error (${market}): ${error.message}`);
              } else {
                eventLines += records.length;
              }
            } catch (e) {
              console.log(`   ❌ Upsert threw (${market}): ${e?.message || e}`);
            }
          } else {
            // Dry run: count only
            eventLines += records.length;
          }
        } else {
          console.log(`   ⛔ ${market}: no lines`);
        }
        
        console.log(`   ✅ ${market}: ${records.length} lines`);
      }
      
      if (eventLines > 0) {
        withData++;
        totalInserted += eventLines;
        console.log(`   🧮 Event total: ${eventLines}`);
      } else {
        noData++;
        console.log(`   ⚠️ No lines for event`);
      }
    }
    
    const result = {
      success: true,
      message: 'Completed',
      eventsProcessed: events.length,
      eventsWithData: withData,
      eventsWithNoData: noData,
      totalLinesInserted: totalInserted,
      timestamp: new Date().toISOString(),
      testMode
    };
    
    console.log('\n🎉 NFL ODDS ALTERNATE LINES UPDATER FINISHED!');
    console.log(`✅ ${result.eventsProcessed} events processed`);
    console.log(`📊 ${result.totalLinesInserted} total lines inserted`);
    
    return result;
    
  } catch (err) {
    console.error('❌ Fatal error:', err?.message || err);
    return {
      success: false,
      error: String(err?.message || err),
      timestamp: new Date().toISOString()
    };
  }
}

// Run the updater
runUpdater()
  .then(result => {
    console.log('\n📋 Final Result:', JSON.stringify(result, null, 2));
    process.exit(result.success ? 0 : 1);
  })
  .catch(error => {
    console.error('❌ Fatal error:', error);
    process.exit(1);
  });