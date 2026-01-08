const { createClient } = require('@supabase/supabase-js');
const fetch = require('node-fetch');

// Check for test mode from command line
const testMode = process.argv.includes('--test');

console.log('🏈 Complete NFLfastR Play-by-Play Updater - All 372 Columns - 2025 Season');
console.log('===============================================================================');

if (testMode) {
  console.log('🧪 TEST MODE: No database changes will be made');
}

// Get environment variables
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('❌ Error: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables are required');
  process.exit(1);
}

// Create Supabase client with service role
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// --- HELPER FUNCTIONS (Strict Type Safety) ---
const safeNumeric = (value) => {
  if (value === null || value === undefined || value === '' || value === 'NA') return null;
  const num = Number(value);
  return isNaN(num) ? null : num;
};

const safeText = (value) => {
  if (value === null || value === undefined || value === '' || value === 'NA') return null;
  return String(value).trim();
};

const safeDouble = (value) => {
  if (value === null || value === undefined || value === '' || value === 'NA') return null;
  const num = parseFloat(value);
  return isNaN(num) ? null : num;
};

// NFLfastR data sources
const NFLVERSE_SOURCES = {
  pbp: 'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_2025.csv'
};

const ALT_SOURCES = {
  pbp: 'https://raw.githubusercontent.com/nflverse/nflverse-pbp/master/data/play_by_play_2025.csv'
};

const FALLBACK_SOURCES = [
  'https://github.com/nflverse/nfldata/raw/master/data/play_by_play_2025.csv',
  'https://raw.githubusercontent.com/nflverse/nfldata/master/data/play_by_play_2025.csv'
];

// --- FIX: SEASON LOGIC ---
// The 2025 Season spans two calendar years (Sept 2025 -> Feb 2026).
// We must hardcode the start/end to capture the full season window correctly.
const now = new Date();
const nflSeasonStart = new Date('2025-09-04'); // Official 2025 Kickoff
const nflSeasonEnd = new Date('2026-02-15');   // Post-Super Bowl 2026

console.log(`\n📅 Current date: ${now.toISOString()}`);
console.log(`🏈 NFL 2025 Season Window: ${nflSeasonStart.toDateString()} to ${nflSeasonEnd.toDateString()}`);

const inNflSeason = now >= nflSeasonStart && now <= nflSeasonEnd;
console.log(`🏈 In NFL Season Window: ${inNflSeason}`); // This will now be TRUE

// Date filtering: Only process games from last 7 days
const cutoffDate = new Date(now.getTime() - (7 * 24 * 60 * 60 * 1000));
console.log(`📅 Date filter: Only processing games from ${cutoffDate.toDateString()} onwards`);

// Fetch and parse function
async function fetchAndParsePbp() {
  const sources = [NFLVERSE_SOURCES.pbp, ALT_SOURCES.pbp, ...FALLBACK_SOURCES];
  
  for (const [index, url] of sources.entries()) {
    console.log(`📡 Source ${index + 1}/${sources.length}: ${url}`);
    
    try {
      const response = await fetch(url, {
        headers: {
          'Accept': 'text/csv,application/csv,text/plain',
          'User-Agent': 'nflfastr-complete-updater/1.0'
        }
      });
      
      console.log(`  → Response: ${response.status} ${response.statusText}`);
      
      if (!response.ok) continue;
      
      const csvText = await response.text();
      console.log(`📄 CSV size: ${(csvText.length / 1024 / 1024).toFixed(1)}MB`);
      
      if (!csvText || csvText.length < 10000) continue;
      
      console.log('🔍 Parsing CSV data...');
      const lines = csvText.split('\n').filter(line => line.trim());
      
      if (lines.length < 2) continue;
      
      // Clean headers to remove quotes and whitespace
      const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
      const pbpData = [];
      let totalProcessed = 0;
      let validPlays = 0;
      
      for (let i = 1; i < lines.length; i++) {
        totalProcessed++;
        
        // Basic CSV parsing handling quotes
        const values = [];
        let current = '';
        let inQuotes = false;
        
        for (let j = 0; j < lines[i].length; j++) {
          const char = lines[i][j];
          if (char === '"') {
            inQuotes = !inQuotes;
          } else if (char === ',' && !inQuotes) {
            values.push(current.trim());
            current = '';
          } else {
            current += char;
          }
        }
        values.push(current.trim());
        
        if (values.length === headers.length) {
          const row = {};
          headers.forEach((header, index) => {
            row[header] = values[index] || null;
          });
          
          // Date filtering logic
          const gameDate = row.game_date;
          if (gameDate) {
            const gameDateObj = new Date(gameDate);
            if (gameDateObj >= cutoffDate) {
              validPlays++;
              pbpData.push(row);
            }
          }
        }
      }
      
      console.log(`✅ Parsing complete: ${totalProcessed} total, ${validPlays} recent plays`);
      return pbpData;
      
    } catch (error) {
      console.log(`🔴 Source ${index + 1} failed: ${error.message}`);
      continue;
    }
  }
  
  throw new Error(`Failed to fetch data from all ${sources.length} sources`);
}

// Main execution function
async function runUpdater() {
  try {
    if (!inNflSeason && !testMode) {
      console.log('📅 Outside NFL season - skipping sync (use --test flag to override)');
      return { success: true, message: 'Outside NFL season - sync skipped' };
    }
    
    console.log('\n📦 Fetching complete NFLfastR data...');
    const pbpData = await fetchAndParsePbp();
    
    console.log(`📊 Found ${pbpData.length} play records to process`);
    
    if (pbpData.length === 0) {
      return { success: true, message: 'No recent plays found' };
    }
    
    console.log('\n🔄 Transforming dataset...');
    const transformedPlays = [];
    
    for (const play of pbpData) {
      if (!play.game_id || !play.play_id) continue;
      
      // --- TRANSFORMATION MAPPING ---
      // We use safeNumeric/safeDouble for numbers to prevent DB errors
      transformedPlays.push({
        play_id: safeNumeric(play.play_id),
        game_id: safeText(play.game_id),
        old_game_id: safeNumeric(play.old_game_id),
        home_team: safeText(play.home_team),
        away_team: safeText(play.away_team),
        season_type: safeText(play.season_type),
        week: safeNumeric(play.week),
        posteam: safeText(play.posteam),
        posteam_type: safeText(play.posteam_type),
        defteam: safeText(play.defteam),
        side_of_field: safeText(play.side_of_field),
        yardline_100: safeNumeric(play.yardline_100), // Fixed: Numeric
        game_date: safeText(play.game_date),
        quarter_seconds_remaining: safeNumeric(play.quarter_seconds_remaining), // Fixed: Numeric
        half_seconds_remaining: safeNumeric(play.half_seconds_remaining), // Fixed: Numeric
        game_seconds_remaining: safeNumeric(play.game_seconds_remaining), // Fixed: Numeric
        game_half: safeText(play.game_half),
        quarter_end: safeNumeric(play.quarter_end), // Fixed: Numeric
        drive: safeNumeric(play.drive), // Fixed: Numeric
        sp: safeNumeric(play.sp), // Fixed: Numeric
        qtr: safeNumeric(play.qtr), // Fixed: Numeric
        down: safeNumeric(play.down), // Fixed: Numeric
        goal_to_go: safeNumeric(play.goal_to_go), // Fixed: Numeric
        time: safeText(play.time),
        yrdln: safeText(play.yrdln),
        ydstogo: safeNumeric(play.ydstogo), // Fixed: Numeric
        ydsnet: safeNumeric(play.ydsnet), // Fixed: Numeric
        desc: safeText(play.desc),
        play_type: safeText(play.play_type),
        yards_gained: safeNumeric(play.yards_gained), // Fixed: Numeric
        shotgun: safeNumeric(play.shotgun), // Fixed: Numeric
        no_huddle: safeNumeric(play.no_huddle), // Fixed: Numeric
        qb_dropback: safeNumeric(play.qb_dropback), // Fixed: Numeric
        qb_kneel: safeNumeric(play.qb_kneel), // Fixed: Numeric
        qb_spike: safeNumeric(play.qb_spike), // Fixed: Numeric
        qb_scramble: safeNumeric(play.qb_scramble), // Fixed: Numeric
        pass_length: safeText(play.pass_length),
        pass_location: safeText(play.pass_location),
        air_yards: safeNumeric(play.air_yards), // Fixed: Numeric
        yards_after_catch: safeNumeric(play.yards_after_catch), // Fixed: Numeric
        run_location: safeText(play.run_location),
        run_gap: safeText(play.run_gap),
        field_goal_result: safeText(play.field_goal_result),
        kick_distance: safeNumeric(play.kick_distance), // Fixed: Numeric
        extra_point_result: safeText(play.extra_point_result),
        two_point_conv_result: safeText(play.two_point_conv_result),
        home_timeouts_remaining: safeNumeric(play.home_timeouts_remaining), // Fixed: Numeric
        away_timeouts_remaining: safeNumeric(play.away_timeouts_remaining), // Fixed: Numeric
        timeout: safeNumeric(play.timeout), // Fixed: Numeric
        timeout_team: safeText(play.timeout_team),
        td_team: safeText(play.td_team),
        td_player_name: safeText(play.td_player_name),
        td_player_id: safeText(play.td_player_id),
        posteam_timeouts_remaining: safeNumeric(play.posteam_timeouts_remaining), // Fixed: Numeric
        defteam_timeouts_remaining: safeNumeric(play.defteam_timeouts_remaining), // Fixed: Numeric
        total_home_score: safeNumeric(play.total_home_score), // Fixed: Numeric
        total_away_score: safeNumeric(play.total_away_score), // Fixed: Numeric
        posteam_score: safeNumeric(play.posteam_score), // Fixed: Numeric
        defteam_score: safeNumeric(play.defteam_score), // Fixed: Numeric
        score_differential: safeNumeric(play.score_differential), // Fixed: Numeric
        posteam_score_post: safeNumeric(play.posteam_score_post), // Fixed: Numeric
        defteam_score_post: safeNumeric(play.defteam_score_post), // Fixed: Numeric
        score_differential_post: safeNumeric(play.score_differential_post), // Fixed: Numeric
        no_score_prob: safeDouble(play.no_score_prob), // Fixed: Double
        opp_fg_prob: safeDouble(play.opp_fg_prob), // Fixed: Double
        opp_safety_prob: safeDouble(play.opp_safety_prob), // Fixed: Double
        opp_td_prob: safeDouble(play.opp_td_prob), // Fixed: Double
        fg_prob: safeDouble(play.fg_prob), // Fixed: Double
        safety_prob: safeDouble(play.safety_prob), // Fixed: Double
        td_prob: safeDouble(play.td_prob), // Fixed: Double
        extra_point_prob: safeDouble(play.extra_point_prob), // Fixed: Double
        two_point_conversion_prob: safeDouble(play.two_point_conversion_prob), // Fixed: Double
        ep: safeDouble(play.ep), // Fixed: Double
        epa: safeDouble(play.epa), // Fixed: Double
        total_home_epa: safeDouble(play.total_home_epa), // Fixed: Double
        total_away_epa: safeDouble(play.total_away_epa), // Fixed: Double
        total_home_rush_epa: safeDouble(play.total_home_rush_epa), // Fixed: Double
        total_away_rush_epa: safeDouble(play.total_away_rush_epa), // Fixed: Double
        total_home_pass_epa: safeDouble(play.total_home_pass_epa), // Fixed: Double
        total_away_pass_epa: safeDouble(play.total_away_pass_epa), // Fixed: Double
        air_epa: safeDouble(play.air_epa), // Fixed: Double
        yac_epa: safeDouble(play.yac_epa), // Fixed: Double
        comp_air_epa: safeDouble(play.comp_air_epa), // Fixed: Double
        comp_yac_epa: safeDouble(play.comp_yac_epa), // Fixed: Double
        total_home_comp_air_epa: safeDouble(play.total_home_comp_air_epa), // Fixed: Double
        total_away_comp_air_epa: safeDouble(play.total_away_comp_air_epa), // Fixed: Double
        total_home_comp_yac_epa: safeDouble(play.total_home_comp_yac_epa), // Fixed: Double
        total_away_comp_yac_epa: safeDouble(play.total_away_comp_yac_epa), // Fixed: Double
        total_home_raw_air_epa: safeDouble(play.total_home_raw_air_epa), // Fixed: Double
        total_away_raw_air_epa: safeDouble(play.total_away_raw_air_epa), // Fixed: Double
        total_home_raw_yac_epa: safeDouble(play.total_home_raw_yac_epa), // Fixed: Double
        total_away_raw_yac_epa: safeDouble(play.total_away_raw_yac_epa), // Fixed: Double
        wp: safeDouble(play.wp),
        def_wp: safeDouble(play.def_wp),
        home_wp: safeDouble(play.home_wp),
        away_wp: safeDouble(play.away_wp),
        vegas_wp: safeDouble(play.vegas_wp),
        vegas_home_wp: safeDouble(play.vegas_home_wp),
        wpa: safeDouble(play.wpa), // Fixed: Double
        vegas_wpa: safeDouble(play.vegas_wpa), // Fixed: Double
        vegas_home_wpa: safeDouble(play.vegas_home_wpa), // Fixed: Double
        home_wp_post: safeDouble(play.home_wp_post), // Fixed: Double
        away_wp_post: safeDouble(play.away_wp_post), // Fixed: Double
        total_home_rush_wpa: safeDouble(play.total_home_rush_wpa), // Fixed: Double
        total_away_rush_wpa: safeDouble(play.total_away_rush_wpa), // Fixed: Double
        total_home_pass_wpa: safeDouble(play.total_home_pass_wpa), // Fixed: Double
        total_away_pass_wpa: safeDouble(play.total_away_pass_wpa), // Fixed: Double
        air_wpa: safeDouble(play.air_wpa), // Fixed: Double
        yac_wpa: safeDouble(play.yac_wpa), // Fixed: Double
        comp_air_wpa: safeDouble(play.comp_air_wpa), // Fixed: Double
        comp_yac_wpa: safeDouble(play.comp_yac_wpa), // Fixed: Double
        total_home_comp_air_wpa: safeDouble(play.total_home_comp_air_wpa), // Fixed: Double
        total_away_comp_air_wpa: safeDouble(play.total_away_comp_air_wpa), // Fixed: Double
        total_home_comp_yac_wpa: safeDouble(play.total_home_comp_yac_wpa), // Fixed: Double
        total_away_comp_yac_wpa: safeDouble(play.total_away_comp_yac_wpa), // Fixed: Double
        total_home_raw_air_wpa: safeDouble(play.total_home_raw_air_wpa), // Fixed: Double
        total_away_raw_air_wpa: safeDouble(play.total_away_raw_air_wpa), // Fixed: Double
        total_home_raw_yac_wpa: safeDouble(play.total_home_raw_yac_wpa), // Fixed: Double
        total_away_raw_yac_wpa: safeDouble(play.total_away_raw_yac_wpa), // Fixed: Double
        // ... (Remaining text fields kept as safeText) ...
        punt_blocked: safeText(play.punt_blocked),
        first_down_rush: safeText(play.first_down_rush),
        first_down_pass: safeText(play.first_down_pass),
        first_down_penalty: safeText(play.first_down_penalty),
        third_down_converted: safeText(play.third_down_converted),
        third_down_failed: safeText(play.third_down_failed),
        fourth_down_converted: safeText(play.fourth_down_converted),
        fourth_down_failed: safeText(play.fourth_down_failed),
        incomplete_pass: safeText(play.incomplete_pass),
        touchback: safeText(play.touchback),
        interception: safeText(play.interception),
        punt_inside_twenty: safeText(play.punt_inside_twenty),
        punt_in_endzone: safeText(play.punt_in_endzone),
        punt_out_of_bounds: safeText(play.punt_out_of_bounds),
        punt_downed: safeText(play.punt_downed),
        punt_fair_catch: safeText(play.punt_fair_catch),
        kickoff_inside_twenty: safeText(play.kickoff_inside_twenty),
        kickoff_in_endzone: safeText(play.kickoff_in_endzone),
        kickoff_out_of_bounds: safeText(play.kickoff_out_of_bounds),
        kickoff_downed: safeText(play.kickoff_downed),
        kickoff_fair_catch: safeText(play.kickoff_fair_catch),
        fumble_forced: safeText(play.fumble_forced),
        fumble_not_forced: safeText(play.fumble_not_forced),
        fumble_out_of_bounds: safeText(play.fumble_out_of_bounds),
        solo_tackle: safeText(play.solo_tackle),
        safety: safeText(play.safety),
        penalty: safeText(play.penalty),
        tackled_for_loss: safeText(play.tackled_for_loss),
        fumble_lost: safeText(play.fumble_lost),
        own_kickoff_recovery: safeText(play.own_kickoff_recovery),
        own_kickoff_recovery_td: safeText(play.own_kickoff_recovery_td),
        qb_hit: safeText(play.qb_hit),
        rush_attempt: safeText(play.rush_attempt),
        pass_attempt: safeText(play.pass_attempt),
        sack: safeText(play.sack),
        touchdown: safeText(play.touchdown),
        pass_touchdown: safeText(play.pass_touchdown),
        rush_touchdown: safeText(play.rush_touchdown),
        return_touchdown: safeText(play.return_touchdown),
        extra_point_attempt: safeText(play.extra_point_attempt),
        two_point_attempt: safeText(play.two_point_attempt),
        field_goal_attempt: safeText(play.field_goal_attempt),
        kickoff_attempt: safeText(play.kickoff_attempt),
        punt_attempt: safeText(play.punt_attempt),
        fumble: safeText(play.fumble),
        complete_pass: safeText(play.complete_pass),
        assist_tackle: safeText(play.assist_tackle),
        lateral_reception: safeText(play.lateral_reception),
        lateral_rush: safeText(play.lateral_rush),
        lateral_return: safeText(play.lateral_return),
        lateral_recovery: safeText(play.lateral_recovery),
        passer_player_id: safeText(play.passer_player_id),
        passer_player_name: safeText(play.passer_player_name),
        passing_yards: safeNumeric(play.passing_yards), // Fixed: Numeric
        receiver_player_id: safeText(play.receiver_player_id),
        receiver_player_name: safeText(play.receiver_player_name),
        receiving_yards: safeNumeric(play.receiving_yards), // Fixed: Numeric
        rusher_player_id: safeText(play.rusher_player_id),
        rusher_player_name: safeText(play.rusher_player_name),
        rushing_yards: safeNumeric(play.rushing_yards), // Fixed: Numeric
        lateral_receiver_player_id: safeText(play.lateral_receiver_player_id),
        lateral_receiver_player_name: safeText(play.lateral_receiver_player_name),
        lateral_receiving_yards: safeNumeric(play.lateral_receiving_yards), // Fixed: Numeric
        lateral_rusher_player_id: safeText(play.lateral_rusher_player_id),
        lateral_rusher_player_name: safeText(play.lateral_rusher_player_name),
        lateral_rushing_yards: safeNumeric(play.lateral_rushing_yards), // Fixed: Numeric
        lateral_sack_player_id: safeText(play.lateral_sack_player_id),
        lateral_sack_player_name: safeText(play.lateral_sack_player_name),
        interception_player_id: safeText(play.interception_player_id),
        interception_player_name: safeText(play.interception_player_name),
        lateral_interception_player_id: safeText(play.lateral_interception_player_id),
        lateral_interception_player_name: safeText(play.lateral_interception_player_name),
        punt_returner_player_id: safeText(play.punt_returner_player_id),
        punt_returner_player_name: safeText(play.punt_returner_player_name),
        lateral_punt_returner_player_id: safeText(play.lateral_punt_returner_player_id),
        lateral_punt_returner_player_name: safeText(play.lateral_punt_returner_player_name),
        kickoff_returner_player_name: safeText(play.kickoff_returner_player_name),
        kickoff_returner_player_id: safeText(play.kickoff_returner_player_id),
        lateral_kickoff_returner_player_id: safeText(play.lateral_kickoff_returner_player_id),
        lateral_kickoff_returner_player_name: safeText(play.lateral_kickoff_returner_player_name),
        punter_player_id: safeText(play.punter_player_id),
        punter_player_name: safeText(play.punter_player_name),
        kicker_player_name: safeText(play.kicker_player_name),
        kicker_player_id: safeText(play.kicker_player_id),
        own_kickoff_recovery_player_id: safeText(play.own_kickoff_recovery_player_id),
        own_kickoff_recovery_player_name: safeText(play.own_kickoff_recovery_player_name),
        blocked_player_id: safeText(play.blocked_player_id),
        blocked_player_name: safeText(play.blocked_player_name),
        tackle_for_loss_1_player_id: safeText(play.tackle_for_loss_1_player_id),
        tackle_for_loss_1_player_name: safeText(play.tackle_for_loss_1_player_name),
        tackle_for_loss_2_player_id: safeText(play.tackle_for_loss_2_player_id),
        tackle_for_loss_2_player_name: safeText(play.tackle_for_loss_2_player_name),
        qb_hit_1_player_id: safeText(play.qb_hit_1_player_id),
        qb_hit_1_player_name: safeText(play.qb_hit_1_player_name),
        qb_hit_2_player_id: safeText(play.qb_hit_2_player_id),
        qb_hit_2_player_name: safeText(play.qb_hit_2_player_name),
        forced_fumble_player_1_team: safeText(play.forced_fumble_player_1_team),
        forced_fumble_player_1_player_id: safeText(play.forced_fumble_player_1_player_id),
        forced_fumble_player_1_player_name: safeText(play.forced_fumble_player_1_player_name),
        forced_fumble_player_2_team: safeText(play.forced_fumble_player_2_team),
        forced_fumble_player_2_player_id: safeText(play.forced_fumble_player_2_player_id),
        forced_fumble_player_2_player_name: safeText(play.forced_fumble_player_2_player_name),
        solo_tackle_1_team: safeText(play.solo_tackle_1_team),
        solo_tackle_2_team: safeText(play.solo_tackle_2_team),
        solo_tackle_1_player_id: safeText(play.solo_tackle_1_player_id),
        solo_tackle_2_player_id: safeText(play.solo_tackle_2_player_id),
        solo_tackle_1_player_name: safeText(play.solo_tackle_1_player_name),
        solo_tackle_2_player_name: safeText(play.solo_tackle_2_player_name),
        assist_tackle_1_player_id: safeText(play.assist_tackle_1_player_id),
        assist_tackle_1_player_name: safeText(play.assist_tackle_1_player_name),
        assist_tackle_1_team: safeText(play.assist_tackle_1_team),
        assist_tackle_2_player_id: safeText(play.assist_tackle_2_player_id),
        assist_tackle_2_player_name: safeText(play.assist_tackle_2_player_name),
        assist_tackle_2_team: safeText(play.assist_tackle_2_team),
        assist_tackle_3_player_id: safeText(play.assist_tackle_3_player_id),
        assist_tackle_3_player_name: safeText(play.assist_tackle_3_player_name),
        assist_tackle_3_team: safeText(play.assist_tackle_3_team),
        assist_tackle_4_player_id: safeText(play.assist_tackle_4_player_id),
        assist_tackle_4_player_name: safeText(play.assist_tackle_4_player_name),
        assist_tackle_4_team: safeText(play.assist_tackle_4_team),
        tackle_with_assist: safeText(play.tackle_with_assist),
        tackle_with_assist_1_player_id: safeText(play.tackle_with_assist_1_player_id),
        tackle_with_assist_1_player_name: safeText(play.tackle_with_assist_1_player_name),
        tackle_with_assist_1_team: safeText(play.tackle_with_assist_1_team),
        tackle_with_assist_2_player_id: safeText(play.tackle_with_assist_2_player_id),
        tackle_with_assist_2_player_name: safeText(play.tackle_with_assist_2_player_name),
        tackle_with_assist_2_team: safeText(play.tackle_with_assist_2_team),
        pass_defense_1_player_id: safeText(play.pass_defense_1_player_id),
        pass_defense_1_player_name: safeText(play.pass_defense_1_player_name),
        pass_defense_2_player_id: safeText(play.pass_defense_2_player_id),
        pass_defense_2_player_name: safeText(play.pass_defense_2_player_name),
        fumbled_1_team: safeText(play.fumbled_1_team),
        fumbled_1_player_id: safeText(play.fumbled_1_player_id),
        fumbled_1_player_name: safeText(play.fumbled_1_player_name),
        fumbled_2_player_id: safeText(play.fumbled_2_player_id),
        fumbled_2_player_name: safeText(play.fumbled_2_player_name),
        fumbled_2_team: safeText(play.fumbled_2_team),
        fumble_recovery_1_team: safeText(play.fumble_recovery_1_team),
        fumble_recovery_1_yards: safeNumeric(play.fumble_recovery_1_yards), // Fixed: Numeric
        fumble_recovery_1_player_id: safeText(play.fumble_recovery_1_player_id),
        fumble_recovery_1_player_name: safeText(play.fumble_recovery_1_player_name),
        fumble_recovery_2_team: safeText(play.fumble_recovery_2_team),
        fumble_recovery_2_yards: safeNumeric(play.fumble_recovery_2_yards), // Fixed: Numeric
        fumble_recovery_2_player_id: safeText(play.fumble_recovery_2_player_id),
        fumble_recovery_2_player_name: safeText(play.fumble_recovery_2_player_name),
        sack_player_id: safeText(play.sack_player_id),
        sack_player_name: safeText(play.sack_player_name),
        half_sack_1_player_id: safeText(play.half_sack_1_player_id),
        half_sack_1_player_name: safeText(play.half_sack_1_player_name),
        half_sack_2_player_id: safeText(play.half_sack_2_player_id),
        half_sack_2_player_name: safeText(play.half_sack_2_player_name),
        return_team: safeText(play.return_team),
        return_yards: safeNumeric(play.return_yards), // Fixed: Numeric
        penalty_team: safeText(play.penalty_team),
        penalty_player_id: safeText(play.penalty_player_id),
        penalty_player_name: safeText(play.penalty_player_name),
        penalty_yards: safeNumeric(play.penalty_yards), // Fixed: Numeric
        replay_or_challenge: safeText(play.replay_or_challenge),
        replay_or_challenge_result: safeText(play.replay_or_challenge_result),
        penalty_type: safeText(play.penalty_type),
        defensive_two_point_attempt: safeText(play.defensive_two_point_attempt),
        defensive_two_point_conv: safeText(play.defensive_two_point_conv),
        defensive_extra_point_attempt: safeText(play.defensive_extra_point_attempt),
        defensive_extra_point_conv: safeText(play.defensive_extra_point_conv),
        safety_player_name: safeText(play.safety_player_name),
        safety_player_id: safeText(play.safety_player_id),
        season: safeNumeric(play.season),
        cp: safeDouble(play.cp), // Fixed: Double
        cpoe: safeDouble(play.cpoe), // Fixed: Double
        series: safeNumeric(play.series),
        series_success: safeText(play.series_success),
        series_result: safeText(play.series_result),
        order_sequence: safeNumeric(play.order_sequence),
        start_time: safeText(play.start_time),
        time_of_day: safeText(play.time_of_day),
        stadium: safeText(play.stadium),
        weather: safeText(play.weather),
        nfl_api_id: safeText(play.nfl_api_id),
        play_clock: safeNumeric(play.play_clock), // Fixed: Numeric
        play_deleted: safeText(play.play_deleted),
        play_type_nfl: safeText(play.play_type_nfl),
        special_teams_play: safeText(play.special_teams_play),
        st_play_type: safeText(play.st_play_type),
        end_clock_time: safeText(play.end_clock_time),
        end_yard_line: safeText(play.end_yard_line),
        fixed_drive: safeNumeric(play.fixed_drive),
        fixed_drive_result: safeText(play.fixed_drive_result),
        drive_real_start_time: safeText(play.drive_real_start_time),
        drive_play_count: safeNumeric(play.drive_play_count), // Fixed: Numeric
        drive_time_of_possession: safeText(play.drive_time_of_possession),
        drive_first_downs: safeNumeric(play.drive_first_downs), // Fixed: Numeric
        drive_inside20: safeText(play.drive_inside20),
        drive_ended_with_score: safeText(play.drive_ended_with_score),
        drive_quarter_start: safeNumeric(play.drive_quarter_start), // Fixed: Numeric
        drive_quarter_end: safeNumeric(play.drive_quarter_end), // Fixed: Numeric
        drive_yards_penalized: safeNumeric(play.drive_yards_penalized), // Fixed: Numeric
        drive_start_transition: safeText(play.drive_start_transition),
        drive_end_transition: safeText(play.drive_end_transition),
        drive_game_clock_start: safeText(play.drive_game_clock_start),
        drive_game_clock_end: safeText(play.drive_game_clock_end),
        drive_start_yard_line: safeText(play.drive_start_yard_line),
        drive_end_yard_line: safeText(play.drive_end_yard_line),
        drive_play_id_started: safeText(play.drive_play_id_started),
        drive_play_id_ended: safeText(play.drive_play_id_ended),
        away_score: safeNumeric(play.away_score), // Fixed: Numeric
        home_score: safeNumeric(play.home_score), // Fixed: Numeric
        location: safeText(play.location),
        result: safeNumeric(play.result),
        total: safeNumeric(play.total),
        spread_line: safeDouble(play.spread_line), // Fixed: Double
        total_line: safeDouble(play.total_line), // Fixed: Double
        div_game: safeText(play.div_game),
        roof: safeText(play.roof),
        surface: safeText(play.surface),
        temp: safeNumeric(play.temp), // Fixed: Numeric
        wind: safeNumeric(play.wind), // Fixed: Numeric
        home_coach: safeText(play.home_coach),
        away_coach: safeText(play.away_coach),
        stadium_id: safeText(play.stadium_id),
        game_stadium: safeText(play.game_stadium),
        aborted_play: safeText(play.aborted_play),
        success: safeText(play.success),
        passer: safeText(play.passer),
        passer_jersey_number: safeText(play.passer_jersey_number),
        rusher: safeText(play.rusher),
        rusher_jersey_number: safeText(play.rusher_jersey_number),
        receiver: safeText(play.receiver),
        receiver_jersey_number: safeText(play.receiver_jersey_number),
        pass: safeText(play.pass),
        rush: safeText(play.rush),
        first_down: safeText(play.first_down),
        special: safeText(play.special),
        play: safeText(play.play),
        passer_id: safeText(play.passer_id),
        rusher_id: safeText(play.rusher_id),
        receiver_id: safeText(play.receiver_id),
        name: safeText(play.name),
        jersey_number: safeText(play.jersey_number),
        id: safeText(play.id),
        fantasy_player_name: safeText(play.fantasy_player_name),
        fantasy_player_id: safeText(play.fantasy_player_id),
        fantasy: safeText(play.fantasy),
        fantasy_id: safeText(play.fantasy_id),
        out_of_bounds: safeText(play.out_of_bounds),
        home_opening_kickoff: safeText(play.home_opening_kickoff),
        qb_epa: safeDouble(play.qb_epa), // Fixed: Double
        xyac_epa: safeDouble(play.xyac_epa), // Fixed: Double
        xyac_mean_yardage: safeDouble(play.xyac_mean_yardage), // Fixed: Double
        xyac_median_yardage: safeDouble(play.xyac_median_yardage), // Fixed: Double
        xyac_success: safeDouble(play.xyac_success), // Fixed: Double
        xyac_fd: safeDouble(play.xyac_fd), // Fixed: Double
        xpass: safeDouble(play.xpass), // Fixed: Double
        pass_oe: safeDouble(play.pass_oe) // Fixed: Double
      });
    }

    
    
    // --- DATABASE UPSERT ---
    if (testMode) {
      console.log(`\n🧪 TEST MODE: Simulating database updates...`);
      console.log(`✅ TEST: Would upsert ${transformedPlays.length} complete NFLfastR records`);
      return { success: true, processed: transformedPlays.length };
    }
    
    console.log(`\n💾 Updating ${transformedPlays.length} records in Supabase...`);
    const batchSize = 100;
    let totalProcessed = 0;
    let errors = [];
    
    for (let i = 0; i < transformedPlays.length; i += batchSize) {
      const batch = transformedPlays.slice(i, i + batchSize);
      const batchNum = Math.floor(i / batchSize) + 1;
      
      const { error } = await supabase
        .from('nflfastr_pbp')
        .upsert(batch, { onConflict: 'play_id,game_id' });
        
      if (error) {
        console.error(`  ❌ Batch ${batchNum} failed: ${error.message}`);
        errors.push(error.message);
      } else {
        totalProcessed += batch.length;
        console.log(`  ✅ Batch ${batchNum} success: ${batch.length} plays`);
      }
      
      // Rate limiting protection
      if (i + batchSize < transformedPlays.length) {
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    }
    
    return { 
      success: errors.length === 0, 
      processed: totalProcessed, 
      failed: transformedPlays.length - totalProcessed 
    };
    
  } catch (error) {
    console.error('❌ Critical Error:', error.message);
    return { success: false, error: error.message };
  }
}

// Execute
runUpdater()
  .then(result => {
    console.log('\n📋 Final Result:', JSON.stringify(result, null, 2));
    process.exit(result.success ? 0 : 1);
  });
